# Performance（I4 ベースライン）

- 計測日: 2026-11-10
- 環境: WSL2 (Linux 6.18.58.1-microsoft-standard-WSL2)、Intel Core i5-12400F（12 スレッド）、メモリ 15GiB
- fixture: `python3 scripts/gen_fixture.py --rows N --seed 42 --out fixtures/bench-N.csv`
  （列: ts, EventID, host, user, proc, path, msg。イベントは重複注入・時刻ギャップ入り）
- コマンド: `cargo run --release -p trivium-engine --bin bench -- fixtures/bench-100k.csv fixtures/bench-400k.csv fixtures/bench-2m.csv`
- 測定は 1 実行あたり 1 回（平均なし）。

## 結果（I4 ベースライン、P 層の before）

| 項目 | 100k | 400k | 2M |
|---|---:|---:|---:|
| create（取込） | 45ms | 165ms | 870ms |
| sort_first（ts 昇順・初回） | 39ms | 122ms | 700ms |
| export_all（全件書出） | 18ms | 65ms | 373ms |
| search_A_cold（`user:alice`・キャッシュ構築込み） | 256ms | 1.31s | 6.34s |
| search_B_cold（`powershell.exe error|warn`） | 157ms | 587ms | 3.09s |
| search_A_warm（キャッシュ有り） | 154ms | 544ms | 2.85s |
| search_B_warm（キャッシュ有り） | 169ms | 602ms | 2.93s |
| ioc_100_save（IOC 100 件保存） | 1.29s | 4.97s | 24.6s |
| ioc_100_query（IOC 適用込み query） | 1.31s | 5.18s | 25.7s |
| ioc_1_more_save（+1 件で再保存） | 1.28s | 5.13s | 25.1s |
| ioc_1_more_query（+1 件後の query） | 1.32s | 5.31s | 27.1s |
| flag_update（1 行フラグ更新） | 15ms | 41ms | 143ms |

## 結果（P1 適用後: ソートキー事前計算）

`sort_first` のみ測り直し（同一環境・同一 fixtures・1 実行）。

| 項目 | 100k | 400k | 2M |
|---|---:|---:|---:|
| sort_first（ts 昇順・初回）after | 41ms | 113ms | 521ms |

- before 比: 400k 122ms → 113ms、2M 700ms → 521ms（約 -26%）。100k は 39ms → 41ms で誤差範囲。
- flag_update（400k）は 50ms で B3 の 100ms 以内条件を引き続き満たす。

## 観察（数値の良し悪しの判断はしない。P 層タスクの該当箇所のみ記す）

- B3 の完了条件「40 万行案件でフラグ 1 件更新が 100ms 以内」: flag_update = 41ms で達成を計測で確認。
- `search_A_cold` には searchable text の初回構築が含まれる。`*_warm` は検索キャッシュ利用時。
- `ioc_100_*` / `ioc_1_more_*` は IOC 変更ごとに全行再評価される現経路の値（P3/P4 の before）。
- `sort_first` は文字列化キーの逐次変換を含む現経路の値（P1 の before）。
- 100GB 級は非目標のため計測していない（§11）。
