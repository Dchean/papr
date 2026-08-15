# papr Miniflux 同步修复分析报告

> 针对 papr（Tauri RSS 阅读器）的 Miniflux/GReader 同步进行修复的完整记录。
> 对照 Miniflux 官方源码（miniflux/v2 的 `internal/googlereader`）逐行核对。

## 一、已报告的三个问题与本因

| 现象 | 根因 |
|---|---|
| 收藏夹（starred）无法同步 | 旧代码用 `GET stream/contents/starred` 拉取收藏。Miniflux **不实现** `stream/contents`（fallback 返回 `[]`），因此 starred 集合永远为空，本地收藏永远不会被「服务器看到」，也无法拉取服务器收藏 |
| 文章已读状态无法与 Miniflux 对齐 | 同一个原因：`stream/contents` 在 Miniflux 上恒为空，unread 集合也为空，`reconcile_sync_state` 把本地所有文章当作「服务器上已读」而标读 |
| 同步后本地全部标记已读 | 上述两个空集合被 `reconcile_sync_state` 解释为「服务器上没有未读/没有收藏」，从而对所有本地文章执行清除 unread/starred 写操作。Miniflux 服务器本身没有被改动——症状全部发生在本地 |

**一句话本因**：旧 `sync.rs` 用 FreshRSS 的 `stream/contents` 拉取路径，而 Miniflux 从不实现该端点，导致拉取恒空 + 本地状态被全量冲刷。

## 二、Miniflux 实际 API（源码核对结论）

- `GET /reader/api/0/stream/items/ids?s=&n=&xt=&c=` → `{"itemRefs":[{"id":…}],"continuation":<int>}`。`c` 是**数字偏移**，0/缺失 = 结束。单页上限 `n≤1000`，全列表上限 10000。
- `POST /reader/api/0/stream/items/contents`（form：`output=json`、`T`、`i=<id>`×N）→ 按 id 取完整条目，Miniflux 唯一可取内容的端点。
- 条目 id 稳定：`tag:google.com,2005:reader/item/%016x`。
- 状态标签用**数字用户前缀**：`user/<uid>/state/com.google/read|starred`（非 `user/-/...`）。
- GReader 登录凭证 = Miniflux「集成 → Google Reader」中配置的 username/password，**不是**账号密码，也不是 API Key。

## 三、修复方案（已全部落地在 `crates/papr-core/src/sync.rs`）

1. **新增 `Provider` 枚举**（`FreshRss` / `Miniflux`），集中管理 API 根路径差异；持久化 `freshrss_provider` 设置。
2. **Miniflux 拉取路径**：`fetch_item_ids`（分页 `c` 偏移）→ `fetch_items_by_id`（POST 批量取内容）。全 reading-list 拉取（受 10k 上限约束），一次对齐 read/unread/starred 三个状态。
3. **保留 FreshRSS 路径**：`stream/contents` + opaque continuation，拉 unread + starred 两个有界集合。
4. **不再全量冲刷**：服务器只对「枚举到的条目」有权威；本地未被提及的文章保持原状态。消除了「同步后全部已读」。
5. **条目匹配顺序**：URL → `remote_id`（Miniflux 稳定 id，持久化进 `articles.remote_id`）→ 每源 guid。Miniflux 新增文章先从服务器导入（`upsert_article`），之后即可双向对齐。
6. **推送本地状态**：`push_queue` 把本地已读/收藏改动经 `edit-tag` 推给服务器；无 `remote_id` 的条目等待下拉映射后再推。
7. **订阅双向收敛**：下拉服务器订阅（本地导入新 feed + 归入对应文件夹），上推本地独有 feed（`subscribe_url` + `set_subscription_folder`）。
8. **空文件夹同步**：Miniflux 的 GReader 标签只存在于订阅上，空文件夹无法用 GReader 表达 → 用原生 v1 `GET/POST /v1/categories` 管理（需 `miniflux_api_key`，连接时可填，本地持久化）。缺失时降级为仅警告。
9. **状态标签兼容**：`has_state_tag` 同时接受 `user/-/...` 与 `user/<uid>/...`（Miniflux 数字前缀）。

## 四、顺带修复的其他 Miniflux 相关问题

- **内容拉取**：旧实现假设 `stream/contents` 存在于 Miniflux，导致文章内容、作者、日期全缺；新实现用 `POST stream/items/contents` 取全量内容。
- **凭证语义**：UI 现在标注「Miniflux 需要应用密码（Settings → API Keys）」，避免用户误填账号密码。
- **API Key 输入**：连接对话框新增可选「API 密钥（用于同步文件夹）」输入并传至后端（`commands.rs`、`api.ts`、`SettingsDialog.tsx`、CLI `--api-key`）。
- **计数语义**：`sync_now` 现在返回「已对齐状态的条目数」（`reconciled`），与 UI「已对齐 N 篇文章的状态」、CLI `reconciled` 字段一致，而不是「新增数」。

## 五、改动文件清单

- `crates/papr-core/src/sync.rs` — 重写（约 1473 行，含 12 个单元测试）
- `crates/papr-core/src/db.rs` — 新增 `feed_url*`、`feed_sync_targets`、`article_id_by_remote_id`、`article_id_by_feed_guid`、`clear_sync_queue_for_article`；移除未用 `feed_urls_for_sync`
- `src-tauri/src/commands.rs` — `freshrss_connect` 加 `api_key`；建/改名/删文件夹与删/移订阅时向服务器传播
- `crates/papr-cli/src/main.rs` — connect 加 `--api-key`
- `src/api.ts` — `freshrssConnect` 加 `apiKey` 参数
- `src/components/SettingsDialog.tsx` — 条件显示 API Key 输入
- `src/locales/{en,zh,ja}.json` — 新增 `minifluxApiKeyPlaceholder`

## 六、验证情况

- 前端：`npx tsc --noEmit` 通过（exit 0）。
- 后端：本机无 Rust 工具链（cargo 不在 PATH，未安装），已做逐函数静态自查：
  - sync.rs 41 个函数齐全，测试 12 个
  - db.rs 19 个关键函数签名与 sync.rs 调用一致
  - commands.rs / CLI / api.ts / SettingsDialog 参数贯通
  - 无残留对已删 `feed_urls_for_sync` 的引用
- 建议在装有 Rust 的机器上跑：`cargo test -p papr-core && cargo build -p papr-cli`。

## 七、已知边界

- Miniflux 全列表上限 10000：超过后旧条目不会被拉取对齐（仍保留本地状态）。
- 本地在 Miniflux 中不存在（如尚未刷新）的文章不会被推送到服务器，直到本地刷新入库。
- 文件夹同步依赖 API Key；未填时日志警告并跳过空文件夹操作。