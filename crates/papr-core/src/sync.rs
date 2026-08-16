//! Synchronisation over the Google Reader compatible API.
//!
//! Supports any GReader-compatible backend; today FreshRSS and Miniflux.
//! Protocol is identical (`ClientLogin`, `reader/api/0/edit-tag`,
//! `stream/contents/...`, `com.google/*` tags) — only the API root path
//! differs per provider, so the `Provider` enum centralises that mapping.
//!
//! Flow: `ClientLogin` for an auth token, push any queued local read/starred
//! changes via `edit-tag`, pull the subscription list (to subscribe locally to
//! new server feeds) and push any local-only feeds back to the server (so the
//! two subscription lists converge rather than drifting), then pull the recent
//! reading-list (to reconcile read/starred state, matched to local articles by
//! URL). Changes queued before their article had a `remote_id` are pushed in a
//! second pass right after the pull assigns one — a single pass would either
//! skip them (they never reach the server) or the pull's state write would
//! clobber them (they silently vanish locally).
//!
//! # Provider differences
//!
//! FreshRSS and Miniflux implement the GReader protocol with two divergences
//! that matter here:
//!
//! * **Item fetch**. FreshRSS serves `stream/contents/<stream>` with an opaque
//!   `continuation` string. Miniflux does not implement `stream/contents` at
//!   all — it answers `[]` — and instead exposes `stream/items/ids` (page
//!   through it with the numeric `c` offset) plus `POST stream/items/contents`
//!   (fetch full items by id). The pull below picks the right pair per
//!   provider.
//! * **Item ids**. Miniflux's ids are `tag:google.com,2005:reader/item/<hex>`
//!   and stay stable across syncs — they are persisted as each article's
//!   `remote_id` so local edits can be pushed back precisely. FreshRSS
//!   regenerates ids per fetch, so its items are matched purely by URL.

use crate::db;
use crate::error::{AppError, AppResult};
use crate::ingestion::parse;
use crate::sanitize;
use chrono::{TimeZone, Utc};
use reqwest::{Client, RequestBuilder};
use rusqlite::Connection;
use serde::de::DeserializeOwned;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::Mutex;

/// The writer connection behind an async mutex — what every sync function reads
/// and writes through. The desktop app passes `state.db`; the CLI passes the
/// connection it opened. Paired with a shared [`Client`] for the HTTP calls.
type Db = Mutex<Connection>;

const READ_TAG: &str = "user/-/state/com.google/read";
const STARRED_TAG: &str = "user/-/state/com.google/starred";
const READING_LIST: &str = "user/-/state/com.google/reading-list";
const MAX_PAGES: usize = 50;

/// True when `categories` carries `tag` (either `user/-/...` or the
/// user-specific `user/<id>/...` form Miniflux emits).
fn has_state_tag(categories: &[String], tag: &str) -> bool {
    categories
        .iter()
        .any(|c| c == tag || c.ends_with(tag.trim_start_matches("user/-")))
}

/// Which GReader-compatible backend the user is connected to. The wire
/// protocol is identical; only where the API root sits under the server URL
/// differs (FreshRSS mounts it at `/api/greader.php`, Miniflux serves it at
/// the server root).
#[derive(Clone, Copy)]
enum Provider {
    FreshRss,
    Miniflux,
}

impl Provider {
    /// Path segment to append to the user-supplied server URL to reach the
    /// GReader API root. Miniflux serves `/accounts/ClientLogin` and
    /// `/reader/api/0/...` straight off the server root, so its suffix is
    /// empty.
    fn path_suffix(self) -> &'static str {
        match self {
            Provider::FreshRss => "/api/greader.php",
            Provider::Miniflux => "",
        }
    }

    /// Parse the persisted setting. Missing / unknown → FreshRss, so older
    /// installs (where this setting didn't exist) keep working unchanged.
    fn from_setting(s: Option<&str>) -> Self {
        match s.unwrap_or("").trim() {
            "miniflux" => Provider::Miniflux,
            _ => Provider::FreshRss,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Provider::FreshRss => "freshrss",
            Provider::Miniflux => "miniflux",
        }
    }
}

/// Normalise a user-supplied server URL to its GReader API root for the
/// chosen provider. Idempotent: if the user already typed the full path,
/// don't append it again.
fn greader_base(url: &str, provider: Provider) -> String {
    let t = url.trim().trim_end_matches('/');
    let suffix = provider.path_suffix();
    if t.ends_with(suffix) || t.contains(&format!("{suffix}/")) {
        t.to_string()
    } else {
        format!("{t}{suffix}")
    }
}

/// An authenticated GReader session.
struct Session {
    base: String,
    auth: String,
    token: String,
}

impl Session {
    fn get(&self, http: &Client, path: &str) -> RequestBuilder {
        http.get(format!("{}/reader/api/0/{path}", self.base))
            .header("Authorization", format!("GoogleLogin auth={}", self.auth))
    }
    fn post(&self, http: &Client, path: &str) -> RequestBuilder {
        http.post(format!("{}/reader/api/0/{path}", self.base))
            .header("Authorization", format!("GoogleLogin auth={}", self.auth))
    }
}

/// Send a request, logging a warning with `label` on transport or HTTP error.
/// Returns the (status-checked) response, so callers can still `json()` it.
async fn send_ok(req: RequestBuilder, label: &str) -> AppResult<reqwest::Response> {
    let resp = match req.send().await {
        Ok(resp) => resp,
        Err(e) => {
            log::warn!("sync: {label} request failed: {e}");
            return Err(e.into());
        }
    };
    let status = resp.status();
    if !status.is_success() {
        log::warn!("sync: {label} failed: status={status}");
    }
    Ok(resp.error_for_status()?)
}

/// Decode a JSON response, logging a warning with `label` on failure.
async fn json_ok<T: DeserializeOwned>(resp: reqwest::Response, label: &str) -> AppResult<T> {
    match resp.json().await {
        Ok(value) => Ok(value),
        Err(e) => {
            log::warn!("sync: {label} JSON decode failed: {e}");
            Err(e.into())
        }
    }
}

/// Exchange username + password for a long-lived auth token.
async fn client_login(http: &Client, base: &str, user: &str, pass: &str) -> AppResult<String> {
    let resp = match http
        .post(format!("{base}/accounts/ClientLogin"))
        .form(&[("Email", user), ("Passwd", pass)])
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            log::warn!("sync: ClientLogin request failed for user {user}: {e}");
            return Err(e.into());
        }
    };
    if !resp.status().is_success() {
        log::warn!(
            "sync: ClientLogin failed for user {user}: status={}",
            resp.status()
        );
        return Err(AppError::code("freshrssLoginFailed"));
    }
    let body = resp.text().await?;
    body.lines()
        .find_map(|l| l.strip_prefix("Auth="))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::code("freshrssNoToken"))
}

/// Build a session from an existing auth token by fetching a fresh write
/// (edit-tag) token. Fails fast if the auth token is no longer valid.
async fn session_with_token(http: &Client, base: &str, auth: String) -> AppResult<Session> {
    let resp = http
        .get(format!("{base}/reader/api/0/token"))
        .header("Authorization", format!("GoogleLogin auth={auth}"));
    let token = send_ok(resp, "GET token")
        .await
        .map_err(|_| AppError::code("freshrssLoginFailed"))?
        .text()
        .await?
        .trim()
        .to_string();
    Ok(Session {
        base: base.to_string(),
        auth,
        token,
    })
}

/// Log in with username + password and obtain a full session.
async fn login(http: &Client, base: &str, user: &str, pass: &str) -> AppResult<Session> {
    let auth = client_login(http, base, user, pass).await?;
    session_with_token(http, base, auth).await
}

#[derive(Deserialize)]
struct SubList {
    #[serde(default)]
    subscriptions: Vec<Sub>,
}
#[derive(Deserialize)]
struct Sub {
    #[serde(default)]
    id: String,
    url: Option<String>,
    title: Option<String>,
    #[serde(default)]
    categories: Vec<SubCat>,
}
/// A GReader category ("label") a subscription belongs to. FreshRSS/Miniflux
/// folders surface here; we map the first named one onto a local folder.
#[derive(Deserialize)]
struct SubCat {
    #[serde(default)]
    id: String,
    #[serde(default)]
    label: Option<String>,
}
impl SubCat {
    /// Human folder name for this category. Prefer the explicit `label`,
    /// otherwise derive it from the `user/-/label/NAME` id. `None` for an
    /// unnamed category, so it is skipped rather than creating a blank folder.
    ///
    /// FreshRSS files every feed the user hasn't categorised under a built-in
    /// "Uncategorized" label. That isn't a real folder — mapping it onto a
    /// local one buries every top-level feed in a junk folder that doesn't
    /// match the server's own presentation — so it is treated as no folder.
    fn folder_name(&self) -> Option<String> {
        self.label
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                self.id
                    .rsplit_once("/label/")
                    .map(|(_, n)| n.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .filter(|n| !n.eq_ignore_ascii_case("Uncategorized"))
    }
}

#[derive(Deserialize)]
struct Contents {
    #[serde(default)]
    items: Vec<Item>,
    // Present when the stream has more pages; fed back as `c=` to page on.
    #[serde(default)]
    continuation: Option<String>,
}
/// The `itemRefs` of a `stream/items/ids` response (Miniflux paging).
#[derive(Deserialize)]
struct IdList {
    #[serde(default, rename = "itemRefs")]
    item_refs: Vec<ItemRef>,
    /// Miniflux's continuation is a numeric *offset*, unlike FreshRSS's
    /// opaque token. `0` / absent means the end of the stream.
    ///
    /// Miniflux serialises this field as a JSON *string* (Go's `,string`
    /// tag — `"1000"`), not a number, so a plain `usize` field fails to
    /// decode and kills the whole pull; see `de_continuation`.
    #[serde(default, deserialize_with = "de_continuation")]
    continuation: Option<usize>,
}

/// Deserialise Miniflux's continuation offset: a JSON string (`"1000"`),
/// a plain number (`1000`), `null`, or a missing field (end of stream).
fn de_continuation<'de, D>(de: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(de)?;
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .map(|n| Some(n as usize))
            .ok_or_else(|| D::Error::custom("continuation must be a non-negative integer")),
        serde_json::Value::String(s) if s.is_empty() => Ok(None),
        serde_json::Value::String(s) => s
            .parse()
            .map(Some)
            .map_err(|_| D::Error::custom("continuation is not a number")),
        serde_json::Value::Null => Ok(None),
        other => Err(D::Error::custom(format!(
            "continuation: unexpected value {other}"
        ))),
    }
}
#[derive(Deserialize)]
struct ItemRef {
    id: String,
}
#[derive(Deserialize)]
struct Item {
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    published: Option<i64>,
    #[serde(default)]
    summary: Option<ItemContent>,
    #[serde(default)]
    content: Option<ItemContent>,
    #[serde(default)]
    origin: Option<ItemOrigin>,
    #[serde(default, deserialize_with = "de_vec")]
    categories: Vec<String>,
    #[serde(default, deserialize_with = "de_vec")]
    canonical: Vec<Href>,
    #[serde(default, deserialize_with = "de_vec")]
    alternate: Vec<Href>,
}

/// Deserialise a `Vec` that may arrive as JSON `null` (Go servers marshal
/// nil slices that way). Missing fields fall back to `#[serde(default)]`'s
/// empty vec; `null` becomes an empty vec instead of a decode error.
fn de_vec<'de, D, T>(de: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned,
{
    Ok(Option::<Vec<T>>::deserialize(de)?.unwrap_or_default())
}
#[derive(Deserialize)]
struct ItemContent {
    #[serde(default)]
    content: String,
}
#[derive(Deserialize)]
struct ItemOrigin {
    #[serde(default, rename = "streamId")]
    stream_id: String,
}
#[derive(Deserialize)]
struct Href {
    href: String,
}

/// The canonical URL to match a remote item on. Miniflux reports both
/// `canonical[].href` (the true entry URL) and `alternate[].href`; GReader
/// historically carried the canonical link in `alternate`, so try
/// `canonical` first then `alternate`.
fn item_url(item: &Item) -> Option<String> {
    item_urls(item).into_iter().next()
}

/// Every distinct candidate URL for a remote item, in match priority order:
/// all `canonical[].href`, then all `alternate[].href`. A feed document often
/// carries the entry link only in `alternate` while Miniflux re-emits it as
/// `canonical` — matching on just the first one lets a re-sync insert a second
/// row for the same post. Capturing the whole set lets the caller find an
/// existing article by any of its links, so imports stay idempotent.
fn item_urls(item: &Item) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    item.canonical
        .iter()
        .chain(item.alternate.iter())
        .map(|h| h.href.trim().to_string())
        .filter(|u| !u.is_empty())
        .filter(|u| seen.insert(u.clone()))
        .collect()
}

/// Build a local `NewArticle` from a remote item, so a server-side article
/// the local DB has never seen (e.g. a starred item ingested before the feed
/// was subscribed locally) can be imported with its full content.
fn item_article(item: &Item, url: Option<String>) -> db::NewArticle {
    let html = item
        .content
        .as_ref()
        .or(item.summary.as_ref())
        .map(|c| c.content.trim().to_string())
        .filter(|s| !s.is_empty());
    let body_text = html
        .as_deref()
        .map(sanitize::html_to_text)
        .unwrap_or_default();
    let summary = item
        .summary
        .as_ref()
        .map(|s| sanitize::html_to_text(&s.content))
        .filter(|s| !s.is_empty());
    let published_at = item.published.and_then(|ts| {
        Utc.timestamp_opt(ts, 0)
            .single()
            .map(|dt| parse::clamp_publish_date(dt).to_rfc3339())
    });
    db::NewArticle {
        guid: item.id.clone(),
        url,
        title: item
            .title
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("Untitled")
            .to_string(),
        author: item.author.clone().filter(|s| !s.trim().is_empty()),
        summary,
        content_html: html,
        body_text,
        image_url: None,
        published_at,
        enclosures: Vec::new(),
    }
}

/// Stored GReader connection. We persist the long-lived auth token rather
/// than the password — a leaked token is revocable server-side and can't be
/// replayed against the user's other accounts. `legacy_pass` holds a
/// plaintext password from an older install, awaiting one-time migration.
///
/// `miniflux_api_key` is the user-supplied API key used for Miniflux's native
/// v1 endpoints (categories/folders), which the GReader protocol alone cannot
/// express for empty folders.
struct Creds {
    url: String,
    user: String,
    auth: Option<String>,
    legacy_pass: Option<String>,
    miniflux_api_key: Option<String>,
    provider: Provider,
}

/// Stored GReader credentials, if a server is configured. The setting keys
/// are still named `freshrss_*` for backwards compatibility with installs
/// that predate multi-provider support — the values are provider-agnostic.
async fn creds(db: &Db) -> AppResult<Option<Creds>> {
    let conn = db.lock().await;
    let url = db::get_setting(&conn, "freshrss_url")?.unwrap_or_default();
    let user = db::get_setting(&conn, "freshrss_user")?.unwrap_or_default();
    let nonempty = |k| db::get_setting(&conn, k).map(|v| v.filter(|s| !s.is_empty()));
    let auth = nonempty("freshrss_auth")?;
    let legacy_pass = nonempty("freshrss_pass")?;
    let miniflux_api_key = nonempty("miniflux_api_key")?;
    let provider = Provider::from_setting(db::get_setting(&conn, "freshrss_provider")?.as_deref());
    if url.trim().is_empty() || user.is_empty() || (auth.is_none() && legacy_pass.is_none()) {
        return Ok(None);
    }
    Ok(Some(Creds {
        url,
        user,
        auth,
        legacy_pass,
        miniflux_api_key,
        provider,
    }))
}

/// The configured GReader server URL and provider, or `None` when not
/// connected.
pub async fn connected_url(db: &Db) -> AppResult<Option<(String, String)>> {
    Ok(creds(db)
        .await?
        .map(|c| (c.url, c.provider.as_str().to_string())))
}

/// Persist a verified connection, storing the auth token and never the
/// password (any legacy stored password is also cleared).
async fn persist_session(
    db: &Db,
    url: &str,
    user: &str,
    auth: &str,
    provider: Provider,
) -> AppResult<()> {
    let conn = db.lock().await;
    db::set_setting(&conn, "freshrss_url", url.trim())?;
    db::set_setting(&conn, "freshrss_user", user)?;
    db::set_setting(&conn, "freshrss_auth", auth)?;
    db::set_setting(&conn, "freshrss_pass", "")?;
    db::set_setting(&conn, "freshrss_provider", provider.as_str())?;
    Ok(())
}

/// Verify credentials against the server and, on success, persist them.
pub async fn connect(
    db: &Db,
    http: &Client,
    url: &str,
    user: &str,
    pass: &str,
    provider: Option<&str>,
    api_key: Option<&str>,
) -> AppResult<()> {
    let provider = Provider::from_setting(provider);
    let base = greader_base(url, provider);
    let session = login(http, &base, user, pass).await?; // verifies credentials
    persist_session(db, url, user, &session.auth, provider).await?;
    if matches!(provider, Provider::Miniflux) {
        let conn = db.lock().await;
        db::set_setting(&conn, "miniflux_api_key", api_key.unwrap_or("").trim())?;
    }
    Ok(())
}

/// Forget the stored GReader credentials.
pub async fn disconnect(db: &Db) -> AppResult<()> {
    let conn = db.lock().await;
    for key in [
        "freshrss_url",
        "freshrss_user",
        "freshrss_auth",
        "freshrss_pass",
        "freshrss_provider",
        "miniflux_api_key",
    ] {
        db::set_setting(&conn, key, "")?;
    }
    Ok(())
}

/// Run a full sync if a server is connected. Returns `true` when a sync
/// actually ran, so the caller can refresh the UI for the reconciled state.
pub async fn run_if_connected(db: &Db, http: &Client) -> AppResult<bool> {
    if creds(db).await?.is_some() {
        sync_now(db, http).await.map(|_| true)
    } else {
        Ok(false)
    }
}

async fn session_from_creds(
    db: &Db,
    http: &Client,
    creds: &Creds,
    base: &str,
) -> AppResult<Session> {
    match &creds.auth {
        Some(auth) => session_with_token(http, base, auth.clone()).await,
        None => {
            let pass = creds.legacy_pass.as_deref().unwrap_or_default();
            let session = login(http, base, &creds.user, pass).await?;
            persist_session(db, &creds.url, &creds.user, &session.auth, creds.provider).await?;
            Ok(session)
        }
    }
}

async fn list_subscriptions(session: &Session, http: &Client, label: &str) -> AppResult<SubList> {
    json_ok(
        send_ok(
            session.get(http, "subscription/list?output=json"),
            &format!("GET subscription/list {label}"),
        )
        .await?,
        &format!("subscription/list {label}"),
    )
    .await
}

fn subscription_stream(sub: &Sub, fallback_url: &str) -> String {
    if sub.id.is_empty() {
        format!("feed/{fallback_url}")
    } else {
        sub.id.clone()
    }
}

/// The GReader label tag for a folder name (used for `add`/`remove` in
/// `subscription/edit`).
fn label_tag(name: &str) -> String {
    format!("user/-/label/{}", name.trim())
}

/// The folder tag to remove from a subscription. Prefer the exact id the
/// server reported (e.g. Miniflux's `user/<id>/label/<name>`); fall back to
/// reconstructing it from the folder name.
fn folder_tag(cat: &SubCat) -> Option<String> {
    cat.folder_name().map(|name| {
        if !cat.id.is_empty() && cat.id.contains("/label/") {
            cat.id.clone()
        } else {
            label_tag(&name)
        }
    })
}

async fn subscribe_url(session: &Session, http: &Client, url: &str, label: &str) -> AppResult<()> {
    let stream = format!("feed/{url}");
    send_ok(
        session.post(http, "subscription/edit").form(&[
            ("ac", "subscribe"),
            ("s", stream.as_str()),
            ("T", session.token.as_str()),
        ]),
        &format!("POST subscription/edit subscribe {label}"),
    )
    .await?;
    Ok(())
}

/// Move a subscription into `folder` (when `Some`) and out of every category
/// listed in `remove`, in one `subscription/edit` call.
async fn set_subscription_folder(
    session: &Session,
    http: &Client,
    stream: &str,
    remove: &[String],
    folder: Option<&str>,
    label: &str,
) -> AppResult<()> {
    let mut form = vec![
        ("ac".to_string(), "edit".to_string()),
        ("s".to_string(), stream.to_string()),
        ("T".to_string(), session.token.clone()),
    ];
    if let Some(folder) = folder.map(str::trim).filter(|s| !s.is_empty()) {
        form.push(("a".to_string(), label_tag(folder)));
    }
    for tag in remove {
        form.push(("r".to_string(), tag.clone()));
    }
    send_ok(
        session.post(http, "subscription/edit").form(&form),
        &format!("POST subscription/edit folder {label}"),
    )
    .await?;
    Ok(())
}

async fn unsubscribe_stream(
    session: &Session,
    http: &Client,
    stream: &str,
    label: &str,
) -> AppResult<()> {
    send_ok(
        session.post(http, "subscription/edit").form(&[
            ("ac", "unsubscribe"),
            ("s", stream),
            ("T", session.token.as_str()),
        ]),
        &format!("POST subscription/edit unsubscribe {label}"),
    )
    .await?;
    Ok(())
}

async fn push_state(
    session: &Session,
    http: &Client,
    remote_id: &str,
    field: &str,
    value: bool,
) -> AppResult<()> {
    let tag = if field == "starred" {
        STARRED_TAG
    } else {
        READ_TAG
    };
    let action = if value { "a" } else { "r" };
    send_ok(
        session.post(http, "edit-tag").form(&[
            ("i", remote_id),
            (action, tag),
            ("T", session.token.as_str()),
        ]),
        "POST edit-tag",
    )
    .await?;
    Ok(())
}

/// Best-effort deletion of one server-side subscription by feed URL.
pub async fn unsubscribe_subscription_url(db: &Db, http: &Client, url: &str) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        log::info!("sync: no server connected; local unsubscribe only");
        return Ok(false);
    };
    let base = greader_base(&creds.url, creds.provider);
    let session = session_from_creds(db, http, &creds, &base).await?;
    let subs = list_subscriptions(&session, http, "for unsubscribe").await?;
    let Some(old) = subs
        .subscriptions
        .iter()
        .find(|s| s.url.as_deref() == Some(url))
    else {
        log::info!("sync: subscription URL not found on server: {url}");
        return Ok(false);
    };
    let stream = subscription_stream(old, url);
    unsubscribe_stream(&session, http, &stream, "feed-url").await?;
    log::info!("sync: unsubscribed server from feed: {url}");
    Ok(true)
}

/// Best-effort propagation for a local feed URL edit. Without this, the next
/// sync can pull the old server subscription back into the local DB as a
/// duplicate feed with the same title.
pub async fn replace_subscription_url(
    db: &Db,
    http: &Client,
    old_url: &str,
    new_url: &str,
) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        log::info!("sync: no server connected; local feed URL update only");
        return Ok(false);
    };
    let base = greader_base(&creds.url, creds.provider);
    let session = session_from_creds(db, http, &creds, &base).await?;

    let subs = list_subscriptions(&session, http, "for replace-url").await?;
    let Some(old) = subs
        .subscriptions
        .iter()
        .find(|s| s.url.as_deref() == Some(old_url))
    else {
        log::info!("sync: old feed URL not found on server: {old_url}");
        return Ok(false);
    };

    let stream = subscription_stream(old, old_url);
    unsubscribe_stream(&session, http, &stream, "replace-url").await?;
    subscribe_url(&session, http, new_url, "replace-url").await?;

    log::info!("sync: replaced server subscription URL: {old_url} -> {new_url}");
    Ok(true)
}

// ─────────────────────────── Miniflux native API ───────────────────────────

/// Miniflux's native v1 endpoints use API-key auth and let us manage
/// categories (folders) beyond what the GReader protocol expresses: GReader
/// labels exist only on subscriptions, so an *empty* local folder has no
/// server-side counterpart. These helpers bridge that gap when the user
/// supplied an API key at connect; without one they degrade to a warning and
/// a no-op, and the GReader-only subscription/label sync still works.

#[derive(Serialize)]
struct MinifluxCategoryCreate<'a> {
    title: &'a str,
}

#[derive(Deserialize)]
struct MinifluxCategory {
    id: i64,
    title: String,
}

#[derive(Serialize)]
struct MinifluxCategoryUpdate<'a> {
    title: &'a str,
}

/// The native-API root + key, or `None` when the provider isn't Miniflux or
/// no API key was stored.
fn miniflux_api(creds: &Creds) -> Option<(String, &str)> {
    if !matches!(creds.provider, Provider::Miniflux) {
        return None;
    }
    Some((
        creds.url.trim().trim_end_matches('/').to_string(),
        creds.miniflux_api_key.as_deref()?,
    ))
}

async fn miniflux_categories(creds: &Creds, http: &Client) -> AppResult<Vec<MinifluxCategory>> {
    let Some((base, api_key)) = miniflux_api(creds) else {
        return Ok(Vec::new());
    };
    json_ok(
        send_ok(
            http.get(format!("{base}/v1/categories"))
                .header("X-Auth-Token", api_key),
            "GET miniflux categories",
        )
        .await?,
        "miniflux categories",
    )
    .await
}

fn find_miniflux_category_id(categories: &[MinifluxCategory], name: &str) -> Option<i64> {
    let name = name.trim();
    categories
        .iter()
        .find(|c| c.title.trim().eq_ignore_ascii_case(name))
        .map(|c| c.id)
}

/// Create an empty Miniflux category. GReader labels only exist on
/// subscriptions, so empty local folders need Miniflux's native API.
pub async fn create_remote_folder(db: &Db, http: &Client, name: &str) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        log::info!("sync: no server connected; local folder create only");
        return Ok(false);
    };
    let Some((base, api_key)) = miniflux_api(&creds) else {
        if matches!(creds.provider, Provider::Miniflux) {
            log::warn!("sync: missing Miniflux API key; reconnect Miniflux to sync empty folders");
            return Ok(false);
        }
        log::info!("sync: provider has no native empty folder create");
        return Ok(false);
    };
    if find_miniflux_category_id(&miniflux_categories(&creds, http).await?, name).is_some() {
        return Ok(true);
    }
    send_ok(
        http.post(format!("{base}/v1/categories"))
            .header("X-Auth-Token", api_key)
            .json(&MinifluxCategoryCreate { title: name.trim() }),
        "POST miniflux categories",
    )
    .await?;
    log::info!("sync: created Miniflux category: {}", name.trim());
    Ok(true)
}

pub async fn rename_remote_folder(
    db: &Db,
    http: &Client,
    old_name: &str,
    new_name: &str,
) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        return Ok(false);
    };
    let Some((base, api_key)) = miniflux_api(&creds) else {
        return Ok(false);
    };
    let categories = miniflux_categories(&creds, http).await?;
    let Some(id) = find_miniflux_category_id(&categories, old_name) else {
        log::info!("sync: remote folder not found for rename: {old_name}");
        return Ok(false);
    };
    send_ok(
        http.put(format!("{base}/v1/categories/{id}"))
            .header("X-Auth-Token", api_key)
            .json(&MinifluxCategoryUpdate {
                title: new_name.trim(),
            }),
        "PUT miniflux category",
    )
    .await?;
    log::info!(
        "sync: renamed Miniflux category: {old_name} -> {}",
        new_name.trim()
    );
    Ok(true)
}

pub async fn delete_remote_folder(db: &Db, http: &Client, name: &str) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        return Ok(false);
    };
    let Some((base, api_key)) = miniflux_api(&creds) else {
        return Ok(false);
    };
    let categories = miniflux_categories(&creds, http).await?;
    let Some(id) = find_miniflux_category_id(&categories, name) else {
        log::info!("sync: remote folder not found for delete: {name}");
        return Ok(false);
    };
    send_ok(
        http.delete(format!("{base}/v1/categories/{id}"))
            .header("X-Auth-Token", api_key),
        "DELETE miniflux category",
    )
    .await?;
    log::info!("sync: deleted Miniflux category: {}", name.trim());
    Ok(true)
}

/// Pull Miniflux categories, including empty ones that never appear in
/// GReader subscription labels.
pub async fn sync_remote_folders(db: &Db, http: &Client) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        return Ok(false);
    };
    if matches!(creds.provider, Provider::Miniflux) && creds.miniflux_api_key.is_none() {
        log::warn!("sync: missing Miniflux API key; reconnect Miniflux to sync empty folders");
        return Ok(false);
    }
    let categories = miniflux_categories(&creds, http).await?;
    if categories.is_empty() {
        return Ok(false);
    }
    let mut imported = 0usize;
    {
        let conn = db.lock().await;
        for category in categories {
            if !category.title.trim().is_empty() {
                db::folder_id_by_name(&conn, &category.title)?;
                imported += 1;
            }
        }
    }
    log::info!("sync: synced Miniflux categories={imported}");
    Ok(true)
}

/// Best-effort propagation for moving a local feed into/out of a folder.
pub async fn set_subscription_folder_url(
    db: &Db,
    http: &Client,
    url: &str,
    folder: Option<&str>,
) -> AppResult<bool> {
    let Some(creds) = creds(db).await? else {
        log::info!("sync: no server connected; local feed folder update only");
        return Ok(false);
    };
    let base = greader_base(&creds.url, creds.provider);
    let session = session_from_creds(db, http, &creds, &base).await?;
    let subs = list_subscriptions(&session, http, "for folder").await?;
    let Some(sub) = subs
        .subscriptions
        .iter()
        .find(|s| s.url.as_deref() == Some(url))
    else {
        subscribe_url(&session, http, url, "folder-missing-feed").await?;
        set_subscription_folder(
            &session,
            http,
            &format!("feed/{url}"),
            &[],
            folder,
            "folder-new-feed",
        )
        .await?;
        return Ok(true);
    };
    let stream = subscription_stream(sub, url);
    let keep = folder.map(label_tag);
    let remove: Vec<String> = sub
        .categories
        .iter()
        .filter_map(folder_tag)
        .filter(|tag| keep.as_deref() != Some(tag.as_str()))
        .collect();
    set_subscription_folder(&session, http, &stream, &remove, folder, "folder").await?;
    log::info!("sync: updated server folder for feed: {url} -> {folder:?}");
    Ok(true)
}

// ─────────────────────────── pull / push core ───────────────────────────

/// Push every queued local read/starred change that has a known `remote_id`.
/// Entries without one stay queued until the pull below maps them. Returns
/// how many pushes succeeded.
async fn push_queue(db: &Db, http: &Client, session: &Session, label: &str) -> AppResult<usize> {
    let queue = {
        let conn = db.lock().await;
        db::take_sync_queue(&conn)?
    };
    log::info!("sync: {label}: pushable queued changes={}", queue.len());
    let mut pushed = 0usize;
    let mut failed: Vec<db::SyncEntry> = Vec::new();
    for entry in queue {
        let ok = match push_state(session, http, &entry.remote_id, &entry.field, entry.value).await
        {
            Ok(_) => true,
            Err(e) => {
                log::warn!(
                    "sync: failed to push article state article_id={} remote_id={} field={} value={}: {e}",
                    entry.article_id,
                    entry.remote_id,
                    entry.field,
                    entry.value
                );
                false
            }
        };
        if ok {
            pushed += 1;
        } else {
            failed.push(entry);
        }
    }
    if !failed.is_empty() {
        log::warn!("sync: {} change(s) failed to push, re-queued", failed.len());
        let conn = db.lock().await;
        for entry in &failed {
            let _ = db::requeue_sync(&conn, entry.article_id, &entry.field, entry.value);
        }
    }
    Ok(pushed)
}

/// Local feed URLs the server doesn't already carry, so each can be subscribed
/// remotely. Pure set difference, factored out of `sync_now` so the selection
/// is unit-testable without a live server.
fn feeds_to_push<'a>(
    local: &'a [String],
    server: &std::collections::HashSet<String>,
) -> Vec<&'a str> {
    local
        .iter()
        .filter(|u| !server.contains(*u))
        .map(String::as_str)
        .collect()
}

/// Fetch every item id of a GReader stream via `stream/items/ids`
/// (Miniflux). Paging uses the numeric `c` offset — Miniflux's
/// `continuation` is an integer offset, unlike FreshRSS's opaque token.
/// The returned ids are fed to [`fetch_items_by_id`] in batches.
async fn fetch_item_ids(
    session: &Session,
    http: &Client,
    stream: &str,
    xt: Option<&str>,
) -> AppResult<Vec<String>> {
    const PAGE: &str = "1000";
    let path = "stream/items/ids";
    let mut out = Vec::new();
    let mut offset: Option<usize> = None;
    for _ in 0..MAX_PAGES {
        let mut params: Vec<(&str, String)> = vec![
            ("output", "json".into()),
            ("s", stream.into()),
            ("n", PAGE.into()),
        ];
        if let Some(xt) = xt {
            params.push(("xt", xt.into()));
        }
        if let Some(c) = offset {
            params.push(("c", c.to_string()));
        }
        let page: IdList = json_ok(
            send_ok(
                session.get(http, path).query(&params),
                "GET stream/items/ids",
            )
            .await?,
            "stream/items/ids",
        )
        .await?;
        for item in page.item_refs {
            out.push(item.id);
        }
        // Miniflux's continuation is the next offset (an integer, and a JSON
        // *string* on the wire); a zero / absent continuation means we've
        // reached the end of the stream. Guard against an offset that never
        // advances (a misbehaving server would otherwise loop to MAX_PAGES).
        match page.continuation {
            Some(c) if c > offset.unwrap_or(0) => offset = Some(c),
            _ => break,
        }
    }
    Ok(out)
}

/// Fetch full items by id via `POST stream/items/contents` (Miniflux).
/// The ids come from [`fetch_item_ids`].
async fn fetch_items_by_id(
    session: &Session,
    http: &Client,
    ids: &[String],
) -> AppResult<Vec<Item>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut form = vec![
        ("T".to_string(), session.token.clone()),
        ("output".to_string(), "json".to_string()),
    ];
    for id in ids {
        form.push(("i".to_string(), id.clone()));
    }
    let contents: Contents = json_ok(
        send_ok(
            session.post(http, "stream/items/contents").form(&form),
            "POST stream/items/contents",
        )
        .await?,
        "stream/items/contents",
    )
    .await?;
    Ok(contents.items)
}

/// Fetch every item of a GReader stream, following `continuation` pages.
/// `xt`, when set, is an "exclude tag" filter (e.g. exclude read to get only
/// unread). FreshRSS path — Miniflux uses [`fetch_item_ids`] +
/// [`fetch_items_by_id`] instead.
///
/// Paging is capped: the sets we fetch this way (unread, starred) are bounded
/// by what the user hasn't yet read/has starred — normally a few hundred
/// items — so `MAX_PAGES` pages of `n=1000` (tens of thousands of items) is a
/// generous ceiling that also stops a pathological server from looping us
/// forever.
async fn fetch_stream_items(
    session: &Session,
    http: &Client,
    stream: &str,
    xt: Option<&str>,
) -> AppResult<Vec<Item>> {
    const PAGE: &str = "1000";
    let path = format!("stream/contents/{stream}");
    let mut out = Vec::new();
    let mut cont: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut params: Vec<(&str, &str)> = vec![("output", "json"), ("n", PAGE)];
        if let Some(xt) = xt {
            params.push(("xt", xt));
        }
        if let Some(c) = cont.as_deref() {
            params.push(("c", c));
        }
        let page: Contents = json_ok(
            send_ok(
                session.get(http, &path).query(&params),
                "GET stream/contents",
            )
            .await?,
            "stream/contents",
        )
        .await?;
        out.extend(page.items);
        match page.continuation {
            Some(c) if !c.is_empty() => cont = Some(c),
            _ => break,
        }
    }
    Ok(out)
}

/// Pull the remote items for this provider: Miniflux pages `items/ids` then
/// fetches each batch of ids, FreshRSS pages `stream/contents` directly.
///
/// The server is the source of truth for read/starred state **only among the
/// items it enumerates** — the long tail of local articles the server never
/// mentions keeps its local state. Marking every unmentioned article read
/// would equate "not in the recent page" with "read", which is false for
/// articles fetched locally but not yet ingested server-side — the cause of
/// the "everything got marked read right after sync" bug.
///
/// For Miniflux the whole reading-list is pulled (paginated to the server's
/// 10k-item ceiling), so read/starred state aligns in both directions for
/// everything recent, while articles the server hasn't seen yet keep their
/// local state untouched.
async fn pull_remote_items(
    session: &Session,
    http: &Client,
    provider: Provider,
) -> AppResult<Vec<Item>> {
    if matches!(provider, Provider::Miniflux) {
        // The full reading-list carries every state tag per item — read,
        // unread and starred — so one stream is enough to align all three.
        let ids = fetch_item_ids(session, http, READING_LIST, None).await?;
        log::info!("sync: Miniflux item ids: reading-list={}", ids.len());
        let mut items = Vec::new();
        for chunk in ids.chunks(500) {
            items.extend(fetch_items_by_id(session, http, chunk).await?);
        }
        Ok(items)
    } else {
        // FreshRSS's full reading-list is unbounded, so pull the two bounded
        // sets (unread + starred) — the same reconciliation FreshRSS clients
        // use. The unread page pins the recent unread set; the starred page
        // pins favorites; everything else keeps its local state.
        let mut items =
            fetch_stream_items(session, http, READING_LIST, Some(READ_TAG)).await?;
        items.extend(fetch_stream_items(session, http, STARRED_TAG, None).await?);
        Ok(items)
    }
}

/// The server's authoritative unread and starred URL sets, used to reconcile
/// the long tail: items a pull of only the recent reading-list window never
/// enumerates (Miniflux caps it at ~10k items) would otherwise keep a stale
/// *local unread* state, drifting the local unread count above the server's.
///
/// For Miniflux the reading-list is Window-capped but the per-state stream
/// (unread / starred) is the bounded set a GReader client treats as truth, so
/// fetching those two id sets (then their URLs) is how we capture "everything
/// the server considers unread / starred". For FreshRSS the same two bounded
/// `fetch_stream_items` sets are used directly.
async fn server_state_sets(
    session: &Session,
    http: &Client,
    provider: Provider,
) -> AppResult<(std::collections::HashSet<String>, std::collections::HashSet<String>)> {
    let unread_urls: HashSet<String>;
    let starred_urls: HashSet<String>;
    if matches!(provider, Provider::Miniflux) {
        let unread_ids = fetch_item_ids(session, http, READING_LIST, Some(READ_TAG)).await?;
        let starred_ids = fetch_item_ids(session, http, STARRED_TAG, None).await?;
        let unread = fetch_items_by_id(session, http, &unread_ids).await?;
        let starred = fetch_items_by_id(session, http, &starred_ids).await?;
        unread_urls = unread.iter().filter_map(item_url).collect();
        starred_urls = starred.iter().filter_map(item_url).collect();
    } else {
        let unread = fetch_stream_items(session, http, READING_LIST, Some(READ_TAG)).await?;
        let starred = fetch_stream_items(session, http, STARRED_TAG, None).await?;
        unread_urls = unread.iter().filter_map(item_url).collect();
        starred_urls = starred.iter().filter_map(item_url).collect();
    }
    log::info!(
        "sync: server state sets: unread_urls={} starred_urls={}",
        unread_urls.len(),
        starred_urls.len()
    );
    Ok((unread_urls, starred_urls))
}

/// Push queued changes, then pull subscriptions, remote articles, and
/// read/starred state. Returns how many articles the server's state was
/// applied to (the "aligned" count surfaced by the UI and the CLI's
/// `reconciled` field).
pub async fn sync_now(db: &Db, http: &Client) -> AppResult<usize> {
    let creds = creds(db)
        .await?
        .ok_or_else(|| AppError::code("freshrssNotConnected"))?;
    let base = greader_base(&creds.url, creds.provider);
    log::info!(
        "sync: starting provider={} base={base}",
        creds.provider.as_str()
    );
    let session = session_from_creds(db, http, &creds, &base).await?;

    // 1 ── push: flush queued local read/starred changes whose remote ids are
    // already known. Entries without remote ids stay queued until the pull
    // below maps them.
    let pushed_count = push_queue(db, http, &session, "before pull").await?;
    if let Err(e) = sync_remote_folders(db, http).await {
        log::warn!("sync: failed to sync remote folders: {e}");
    }

    // 2 ── pull subscriptions: subscribe locally to any feed we don't have and
    // keep a remote stream -> local feed map for the item pull below.
    let subs = list_subscriptions(&session, http, "").await?;
    let server_urls: HashSet<String> = subs
        .subscriptions
        .iter()
        .filter_map(|s| s.url.clone())
        .filter(|u| !u.is_empty())
        .collect();
    let mut remote_feed_ids: HashMap<String, i64> = HashMap::new();
    {
        let conn = db.lock().await;
        for sub in subs.subscriptions {
            // Resolve the server-side folder (GReader "label") before moving
            // `url` out of `sub`, mapping it onto a local folder by name.
            let folder_id = sub
                .categories
                .iter()
                .find_map(SubCat::folder_name)
                .map(|name| db::folder_id_by_name(&conn, &name))
                .transpose()?;
            let Some(feed_url) = sub
                .url
                .as_deref()
                .map(str::trim)
                .filter(|u| !u.is_empty())
                .map(str::to_string)
            else {
                continue;
            };
            let feed_id = match db::find_feed_by_url(&conn, &feed_url)? {
                None => {
                    let title = sub.title.clone().unwrap_or_else(|| feed_url.clone());
                    let st = parse::detect_source_type(&feed_url);
                    log::info!("sync: importing server subscription: {title} <{feed_url}>");
                    db::insert_feed(&conn, &feed_url, None, &title, None, st, folder_id)?
                }
                Some(id) => {
                    if db::feed_folder_id(&conn, id)? != folder_id {
                        db::move_feed(&conn, id, folder_id)?;
                    }
                    id
                }
            };
            let stream = subscription_stream(&sub, &feed_url);
            remote_feed_ids.insert(stream.clone(), feed_id);
            // Miniflux reports origin stream ids like `feed/<id>`; keep the
            // URL-keyed stream too so both resolve to the same local feed.
            remote_feed_ids.insert(format!("feed/{feed_url}"), feed_id);
        }
    }

    // 2b ── push subscriptions: subscribe the server to any local feed it
    // doesn't have yet, so adding a feed in the app propagates to the server
    // instead of leaving the two sides to drift. Best-effort and idempotent —
    // re-subscribing a feed the server already has is a no-op there.
    let local_feed_targets = {
        let conn = db.lock().await;
        db::feed_sync_targets(&conn)?
    };
    let local_feed_urls: Vec<String> = local_feed_targets
        .iter()
        .map(|(url, _)| url.clone())
        .collect();
    for url in feeds_to_push(&local_feed_urls, &server_urls) {
        let pushed = match subscribe_url(&session, http, url, "local-only").await {
            Ok(_) => true,
            Err(e) => {
                log::warn!("sync: failed to subscribe server to {url}: {e}");
                false
            }
        };
        if pushed {
            log::info!("sync: subscribed server to local feed: {url}");
            let folder = local_feed_targets
                .iter()
                .find(|(u, _)| u == url)
                .and_then(|(_, folder)| folder.as_deref());
            if folder.is_some() {
                if let Err(e) = set_subscription_folder(
                    &session,
                    http,
                    &format!("feed/{url}"),
                    &[],
                    folder,
                    "local-only",
                )
                .await
                {
                    log::warn!("sync: failed to set server folder for {url}: {e}");
                }
            }
        }
    }

    // 3 ── pull recent remote items and apply their read/starred state. The
    // server is authoritative *for the items it enumerates*: every item in the
    // pulled unread/starred sets gets its exact state written locally, and
    // articles the server didn't mention keep their local state. This matches
    // a pure GReader client's view and avoids the "everything got marked read"
    // trap of treating absence from the recent page as read.
    let items = pull_remote_items(&session, http, creds.provider).await?;

    let mut reconciled = 0usize;
    let mut inserted = 0usize;
    {
        let conn = db.lock().await;
        // Index local articles by remote id once, so a large pull doesn't do a
        // per-item table scan; built lazily on first miss instead of eagerly
        // (the common case is a small unread set).
        for item in items {
            let urls = item_urls(&item);
            // The first candidate stays the "face" URL for new imports; the
            // rest are lookup-only so a canonical/alternate mismatch between
            // the feed document and Miniflux can't create a duplicate row.
            let url = urls.first().cloned();
            let read = has_state_tag(&item.categories, READ_TAG);
            let starred = has_state_tag(&item.categories, STARRED_TAG);
            let feed_id = item
                .origin
                .as_ref()
                .and_then(|o| remote_feed_ids.get(&o.stream_id))
                .copied();
            let mut aid = None;
            for candidate in &urls {
                if let Some(existing) = db::article_id_by_url(&conn, candidate)? {
                    aid = Some(existing);
                    break;
                }
            }
            // Miniflux's item id is stable — prefer the remote-id mapping when
            // the URL lookup missed (URL normalisation differences between the
            // feed document and Miniflux's canonical href).
            if aid.is_none() {
                aid = db::article_id_by_remote_id(&conn, &item.id)?;
            }
            if aid.is_none() {
                if let Some(feed_id) = feed_id {
                    aid = db::article_id_by_feed_guid(&conn, feed_id, &item.id)?;
                }
            }

            let aid = match (aid, feed_id) {
                (Some(aid), _) => aid,
                (None, Some(feed_id)) => {
                    // First sight of a server-side article: import it so a
                    // starred item (or one added on another device) appears
                    // locally even before the feed's own refresh runs.
                    let article = item_article(&item, url);
                    if db::upsert_article(&conn, feed_id, &article, false, &[])? && !read {
                        inserted += 1;
                    }
                    match db::article_id_by_feed_guid(&conn, feed_id, &item.id)? {
                        Some(aid) => aid,
                        None => continue,
                    }
                }
                (None, None) => {
                    log::debug!("sync: skipped remote item without known feed: {}", item.id);
                    continue;
                }
            };

            // The remote id is persisted even when the article has a pending
            // local change, so the push pass right after this pull can send it.
            db::set_remote_id(&conn, aid, &item.id)?;
            // A still-queued local change (e.g. queued before this article had
            // a remote id) must not be overwritten by server state, nor
            // cleared — it is pushed in step 3b below. Overwriting it here
            // would silently drop the user's star/read edit (the "two-way
            // alignment" bug): locally the change looks applied, but it never
            // reaches the server.
            if db::article_has_pending_sync(&conn, aid)? {
                log::debug!(
                    "sync: deferring server state for article {aid} (pending local change)"
                );
                continue;
            }
            db::set_sync_state(&conn, aid, read, starred)?;
            db::clear_sync_queue_for_article(&conn, aid)?;
            reconciled += 1;
        }
    }

    // 3a ── reconcile the long tail. The item loop above only aligns items the
    // server puts in the reading-list window; anything older (Miniflux caps it
    // at ~10k items) keeps a stale local unread state. Sweep every article in a
    // server-known feed against the server's authoritative unread/starred URL
    // sets: items the server considers read are marked read locally, so the
    // local unread count converges on the server's. `reconcile_sync_state`
    // skips articles with an un-pushed local edit, so a two-way edit already
    // queued is never clobbered (same principle as the per-item loop above).
    match server_state_sets(&session, http, creds.provider).await {
        Ok((unread_urls, starred_urls)) => {
            let reconciled_tail = {
                let conn = db.lock().await;
                let server_feed_ids = db::feed_ids_by_urls(&conn, &server_urls)?;
                db::reconcile_sync_state(&conn, &server_feed_ids, &unread_urls, &starred_urls)?
            };
            reconciled += reconciled_tail;
            log::info!("sync: reconciled long-tail articles={reconciled_tail}");
        }
        Err(e) => log::warn!("sync: long-tail reconcile skipped: {e}"),
    }

    // 3b ── second push pass: changes queued before the pull had no remote id
    // and were left in the queue; the pull above just assigned one, so push
    // them now and reach the server in the same sync run.
    let pushed_after_pull = push_queue(db, http, &session, "after pull").await?;

    log::info!(
        "sync: finished; reconciled_articles={reconciled}; inserted_articles={inserted}; pushed_before_pull={pushed_count}; pushed_after_pull={pushed_after_pull}"
    );
    // The count callers surface ("aligned the state of N articles", the CLI's
    // `reconciled` field) is the number of articles the server state was
    // applied to — not just the newly inserted ones.
    Ok(reconciled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat(id: &str, label: Option<&str>) -> SubCat {
        SubCat {
            id: id.to_string(),
            label: label.map(str::to_string),
        }
    }

    #[test]
    fn folder_name_prefers_label() {
        assert_eq!(
            cat("user/-/label/Tech", Some("Tech")).folder_name().as_deref(),
            Some("Tech")
        );
    }

    #[test]
    fn folder_name_falls_back_to_label_id() {
        // Some servers omit the human label; derive it from the id instead.
        assert_eq!(
            cat("user/-/label/科技", None).folder_name().as_deref(),
            Some("科技")
        );
    }

    #[test]
    fn folder_name_skips_unnamed_categories() {
        // A state tag (not a label) or a blank label is not a folder.
        assert_eq!(cat("user/-/state/com.google/read", None).folder_name(), None);
        assert_eq!(cat("", Some("   ")).folder_name(), None);
    }

    #[test]
    fn folder_name_skips_freshrss_uncategorized() {
        // FreshRSS's built-in "Uncategorized" label is not a real folder, by
        // either label or id, and regardless of case.
        assert_eq!(
            cat("user/-/label/Uncategorized", Some("Uncategorized")).folder_name(),
            None
        );
        assert_eq!(cat("user/-/label/uncategorized", None).folder_name(), None);
    }

    #[test]
    fn feeds_to_push_selects_only_local_only_feeds() {
        let local = vec![
            "https://a.example/feed".to_string(),
            "https://b.example/feed".to_string(),
            "https://c.example/feed".to_string(),
        ];
        let server: std::collections::HashSet<String> =
            ["https://b.example/feed".to_string()].into_iter().collect();
        assert_eq!(
            feeds_to_push(&local, &server),
            vec!["https://a.example/feed", "https://c.example/feed"]
        );
    }

    #[test]
    fn feeds_to_push_empty_when_server_has_everything() {
        let local = vec!["https://a.example/feed".to_string()];
        let server: std::collections::HashSet<String> =
            ["https://a.example/feed".to_string()].into_iter().collect();
        assert!(feeds_to_push(&local, &server).is_empty());
    }

    #[test]
    fn state_tag_accepts_numeric_user_prefix() {
        let categories = vec![
            "user/123/state/com.google/read".to_string(),
            "user/-/state/com.google/starred".to_string(),
        ];
        assert!(has_state_tag(&categories, READ_TAG));
        assert!(has_state_tag(&categories, STARRED_TAG));
    }

    #[test]
    fn state_tag_ignores_unrelated_tags() {
        let categories = vec!["user/-/label/Tech".to_string()];
        assert!(!has_state_tag(&categories, READ_TAG));
        assert!(!has_state_tag(&categories, STARRED_TAG));
    }

    #[test]
    fn item_url_prefers_canonical_then_alternate() {
        let item = Item {
            id: "i1".into(),
            title: None,
            author: None,
            published: None,
            summary: None,
            content: None,
            origin: None,
            categories: vec![],
            canonical: vec![Href { href: "https://a.example/x".into() }],
            alternate: vec![Href { href: "https://alt.example/x".into() }],
        };
        assert_eq!(item_url(&item).as_deref(), Some("https://a.example/x"));

        let no_canonical = Item {
            canonical: vec![],
            ..item
        };
        assert_eq!(
            item_url(&no_canonical).as_deref(),
            Some("https://alt.example/x")
        );

        let both_blank = Item {
            canonical: vec![],
            alternate: vec![],
            ..item
        };
        assert_eq!(item_url(&both_blank), None);
    }

    #[test]
    fn item_article_maps_miniflux_fields() {
        let item = Item {
            id: "tag:google.com,2005:reader/item/0000000000000001".into(),
            title: Some("  Hello  ".into()),
            author: Some("Ada".into()),
            published: Some(1_700_000_000),
            summary: None,
            content: Some(ItemContent {
                content: "<p>Body</p>".into(),
            }),
            origin: None,
            categories: vec![],
            canonical: vec![Href {
                href: "https://a.example/x".into(),
            }],
            alternate: vec![],
        };
        let a = item_article(&item, item_url(&item));
        assert_eq!(
            a.guid,
            "tag:google.com,2005:reader/item/0000000000000001"
        );
        assert_eq!(a.title, "Hello");
        assert_eq!(a.author.as_deref(), Some("Ada"));
        assert_eq!(a.content_html.as_deref(), Some("<p>Body</p>"));
        assert!(a.body_text.contains("Body"));
        assert_eq!(a.url.as_deref(), Some("https://a.example/x"));
    }

    #[test]
    fn ids_continuation_accepts_miniflux_string_form() {
        // Miniflux serialises the continuation offset as a JSON string
        // (`json:"continuation,omitempty,string"`); decoding it as a number
        // used to fail the whole pull with "error decoding response body".
        let page: IdList = serde_json::from_str(
            r#"{"itemRefs":[{"id":"1"},{"id":"2"}],"continuation":"1000"}"#,
        )
        .unwrap();
        assert_eq!(page.continuation, Some(1000));
        assert_eq!(page.item_refs.len(), 2);
    }

    #[test]
    fn ids_continuation_accepts_numeric_form_and_missing() {
        // Some servers emit a plain number; the end of the stream omits the
        // field (or sends `0` / `null`).
        let page: IdList =
            serde_json::from_str(r#"{"itemRefs":[{"id":"1"}],"continuation":1000}"#).unwrap();
        assert_eq!(page.continuation, Some(1000));

        let end: IdList = serde_json::from_str(r#"{"itemRefs":[]}"#).unwrap();
        assert_eq!(end.continuation, None);

        let zero: IdList = serde_json::from_str(r#"{"itemRefs":[],"continuation":0}"#).unwrap();
        assert_eq!(zero.continuation, Some(0));

        let null: IdList = serde_json::from_str(r#"{"itemRefs":[],"continuation":null}"#).unwrap();
        assert_eq!(null.continuation, None);
    }
}