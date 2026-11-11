# Performance

## T27 実測（2026-11-11、engine）

- 環境: WSL2 Ubuntu 24.04.4 / Linux 6.18.58.1-microsoft-standard-WSL2、
  Intel Core i5-12400F（12 スレッド）、メモリ 15GiB、Rust 1.97.1、release build。
- fixture: `scripts/gen_fixture.py --rows N --seed 42`（`ascii`）と
  `--rows 100000 --seed 42 --variant long`。`*.csv` はリポジトリに含めない。
- コマンド: `cargo build --release -p trivium-engine --bin bench` の後、
  `./target/release/bench fixtures/bench-100k.csv fixtures/bench-400k.csv fixtures/bench-2m.csv fixtures/bench-long-100k.csv`
- 各 fixture は同じバイナリの子プロセスで実行し、`/proc/self/status` の `VmHWM`
  を fixture ごとに記録した。RSS は Rust プロセスだけである。
- "cold" は「そのプロセスで結果キャッシュが空の最初の要求」を指す。OS の
  ページキャッシュは落としていないため、ディスク cold ではない。
- 短い反復操作は 20 回（n=20）の p50/p95。import と小規模出力は n=3 の
  min/median/max のみで、p95 は出していない。
- キャンセルは「実行前に cancel した `JobContext`」で測った。最初の
  チェックポイントまでの時間であり、バッチ実行途中の cancel 応答ではない。

### fixture 特性

| fixture | 行数 | 列数 | 入力 | parquet | 平均レコード長 |
|---|---:|---:|---:|---:|---:|
| bench-100k | 100,000 | 7 | 9.831MiB | 2.467MiB | 103.1 B |
| bench-400k | 400,000 | 7 | 39.386MiB | 9.777MiB | 103.2 B |
| bench-2m | 2,000,000 | 7 | 197.175MiB | 48.789MiB | 103.4 B |
| bench-long-100k | 100,000 | 120 | 108.341MiB | 37.005MiB | 1136.0 B |

`long` は 120 列。うち 3 列が約 160 文字、残りは短い値で、1 レコード約 1.1KiB。

### import・案件再開

| 指標 | 100k | 400k | 2M | long 100k |
|---|---:|---:|---:|---:|
| import（n=3、median） | 206.2ms | 921.6ms | 4330.5ms | 2382.5ms |
| import（n=3、min–max） | 205.7–239.1ms | 901.6–937.1ms | 4019.8–5504.0ms | 2350.3–2419.4ms |
| 案件再開 p50（n=20） | 380.3ms | 1461.7ms | 7411.1ms | 6978.6ms |
| 案件再開 p95（n=20） | 397.1ms | 1495.2ms | 7749.3ms | 7220.2ms |

案件再開は `ProjectsStore` の作り直しと `load_project` を含む。1 回の
`load_project` が全列 DataFrame を読むため、2M では 7 秒台になる。

### ページ・検索（n=20、cold は n=1）

| 指標 | 100k | 400k | 2M | long 100k |
|---|---:|---:|---:|---:|
| 先頭表示 cold | 4.4ms | 5.8ms | 6.5ms | 50.4ms |
| 先頭表示 warm p50 / p95 | 3.1 / 3.8ms | 6.5 / 10.6ms | 5.8 / 6.1ms | 48.2 / 49.8ms |
| 通常ページ移動 p50 / p95 | 3.2 / 4.2ms | 8.2 / 9.8ms | 6.0 / 6.6ms | 54.1 / 56.5ms |
| sort 後ページ cold | 21.5ms | 106.3ms | 386.7ms | 64.9ms |
| sort 後ページ warm p50 / p95 | 3.1 / 3.6ms | 5.1 / 6.6ms | 6.0 / 6.4ms | 52.8 / 57.3ms |
| 初回検索（全列部分一致 `powershell`） | 98.0ms | 385.9ms | 1981.3ms | 1310.0ms |
| 同じ検索結果の再表示 p50 / p95 | 4.9 / 6.0ms | 7.9 / 9.1ms | 7.9 / 9.4ms | 58.6 / 64.2ms |
| 列スコープ検索 cold（`user:alice`） | 22.3ms | 80.2ms | 413.0ms | 74.4ms |

- 通常ページ移動の `resolve_target` 呼出しは全 fixture で 0 回。
- sort 後ページの warm も `resolve_target` は 0 回。cold は 1 回。
- 同じ検索結果の warm も `resolve_target` は 0 回。初回だけ 1 回。
- 2M の全列部分一致は初回 2.0 秒、long 100k は 1.3 秒である。初回検索も
  任意ソートも「常に瞬時」ではない。

### 注釈・出力・キャンセル

| 指標 | 100k | 400k | 2M | long 100k |
|---|---:|---:|---:|---:|
| フラグ保存 p50 / p95（n=20） | 0.17 / 0.31ms | 0.17 / 0.24ms | 0.15 / 0.33ms | 0.22 / 1.09ms |
| フラグ保存時の全件読込み | 0 | 0 | 0 | 0 |
| 出力 filtered（n=3、median） | 57.5ms | 193.5ms | 944.8ms | 135.9ms |
| 出力 selected 100 行（n=3、median） | 30.4ms | 88.0ms | 391.1ms | 108.3ms |
| Cancel 検索（事前 cancel）p95 | 0.09µs | 0.09µs | 0.09µs | 0.09µs |
| Cancel 出力（事前 cancel）p95 | 0.28µs | 0.18µs | 0.18µs | 1.00µs |

出力 filtered は `user:alice host:WS01` の小規模結果。selected は 100 行。
import と同様、出力は n=3 なので p95 は出していない。

### キャッシュと RSS

| 指標 | 100k | 400k | 2M | long 100k |
|---|---:|---:|---:|---:|
| 結果IDキャッシュ（同一 spec 3 ページ） | resolve 1 回 | 1 回 | 1 回 | 1 回 |
| 保持エントリ / バイト | 1 / 0.763MiB | 1 / 3.052MiB | 1 / 15.259MiB | 1 / 0.763MiB |
| 上限（設定値） | 2 / 64MiB | 2 / 64MiB | 2 / 64MiB | 2 / 64MiB |
| 案件 close 後のキャッシュ | 1→0 / 0MiB | 1→0 / 0MiB | 1→0 / 0MiB | 1→0 / 0MiB |
| import 後 VmHWM | 84.3MiB | 89.9MiB | 101.9MiB | 97.1MiB |
| 再開後 VmHWM | 104.1MiB | 231.4MiB | 973.0MiB | 654.5MiB |
| 検索後 VmHWM | 104.1MiB | 231.4MiB | 973.0MiB | 654.5MiB |
| 出力後 VmHWM | 121.2MiB | 270.5MiB | 1188.2MiB | 679.9MiB |
| close 前 VmHWM | 121.2MiB | 277.6MiB | 1226.4MiB | 687.0MiB |
| close 後 VmHWM / VmRSS | 121.2 / 90.1MiB | 277.6 / 231.5MiB | 1226.4 / 927.8MiB | 687.0 / 573.4MiB |

- 2M の結果IDキャッシュは 2,000,000 行分の ID で 15.259MiB。64MiB 上限内。
- 案件削除で結果IDキャッシュのエントリとバイトは 0 になった。
- ただし `VmHWM` はプロセスの最大値なので close 後も下がらない。`VmRSS` も
  2M では 927.8MiB 残っており、ヒープが OS へ返ったことは確認できていない。
- フロントのページキャッシュ（5 ページ / 16MiB）は設定値
  （`src/lib/components/project_view/state.ts`）を確認しただけで、実 UI の
  保持量は測っていない。

### 目標に対する判定

| 指標 | 目標 | 実測 | 判定 |
|---|---|---|---|
| フラグの画面反映 | 50ms 程度以内 | engine の保存 p95 は最大 1.09ms。UI 反映は未計測 | **未測定**（engine 保存は達成） |
| 1件の注釈保存 | p95 100ms 以内・全件読込み 0 回 | 最大 p95 1.09ms（long）、`project_data_reads` 0 | **達成** |
| 同条件のページ移動 | p95 150ms 以内・検索/ソート再実行 0 回 | 最大 p95 56.5ms（long）、resolve 0 回 | **達成** |
| 同じ検索結果の再表示 | p95 200ms 以内 | 最大 p95 64.2ms（long） | **達成** |
| 検索入力から実行まで | 200ms 前後の debounce | コード上の `setTimeout` は 160ms。実入力の計測なし | **未測定** |
| 中断要求への反応 | 通常バッチで 500ms 以内 | 事前 cancel は p95 最大 1.00µs。バッチ途中の cancel は未計測 | **一部達成 / 未測定** |
| フロントのページキャッシュ | 最大 5 ページ / 16MiB | 設定値のみ確認、実 UI は未計測 | **未測定** |
| 結果IDキャッシュ | 最大 2 結果 / 64MiB | 設定 2 / 64MiB、実測 1 エントリ / 15.259MiB | **達成** |
| 2M 標準 fixture の全体ピーク RSS | 暫定 1GiB 以下 | Rust のみで 1226.4MiB。WebView は未計測 | **未達** |

2M の RSS 未達の主因は、案件再開の `load_project` が全列 DataFrame を読む
こと（再開後 973.0MiB）と、出力が全列 DataFrame を読むこと（出力後
1188.2MiB）。import 自体は 101.9MiB で、ストリーミング取込みは効いている。
WebView プロセスを含めるとさらに増えるため、この値は下限側の記録である。

### リリース判断への注意

- この計測は性能だけの記録である。T26 の実 native GUI 確認は未完了であり、
  原データや注釈を失う不具合が残る状態で、性能を理由にリリース可とはしない。
- 未測定: WebView を含む RSS、UI のフラグ描画、検索入力から要求までの実時間、
  フロントのページキャッシュ保持量、バッチ途中の Cancel 応答。

## 履歴: I4 ベースライン（参考）

- 計測日: 2026-11-10
- 環境: WSL2 (Linux 6.18.58.1-microsoft-standard-WSL2)、Intel Core i5-12400F（12 スレッド）、メモリ 15GiB
- fixture: `python3 scripts/gen_fixture.py --rows N --seed 42 --out fixtures/bench-N.csv`
  （列: ts, EventID, host, user, proc, path, msg。イベントは重複注入・時刻ギャップ入り）
- コマンド: `cargo run --release -p trivium-engine --bin bench -- fixtures/bench-100k.csv fixtures/bench-400k.csv fixtures/bench-2m.csv`
- 測定は 1 実行あたり 1 回（平均なし）。以下の I4 値は T27 の判定には使わない。
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

## 結果（P3 適用後: IOC 末尾追加の増分評価）

`ioc_1_more_save` / `ioc_1_more_query` のみ測り直し（同一環境・同一 fixtures・1 実行）。
コマンド: `cargo run --release -p trivium-engine --bin bench -- fixtures/bench-100k.csv fixtures/bench-400k.csv fixtures/bench-2m.csv`

| 項目 | 100k | 400k | 2M |
|---|---:|---:|---:|
| ioc_1_more_save before → after | 1.28s → 126.9ms | 5.13s → 470.1ms | 25.1s → 2.624s |
| ioc_1_more_query before → after | 1.32s → 116.7ms | 5.31s → 402.1ms | 27.1s → 1.965s |

## 観察（数値の良し悪しの判断はしない。P 層タスクの該当箇所のみ記す）

- B3 の完了条件「40 万行案件でフラグ 1 件更新が 100ms 以内」: flag_update = 41ms で達成を計測で確認。
- `search_A_cold` には searchable text の初回構築が含まれる。`*_warm` は検索キャッシュ利用時。
- `ioc_100_*` / `ioc_1_more_*` の I4 値は IOC 変更ごとに全行再評価していた時点の値
  （P3/P4 の before）。P3 適用後の `ioc_1_more_*` は上節。
- `sort_first` は文字列化キーの逐次変換を含む現経路の値（P1 の before）。
- 100GB 級は非目標のため計測していない（§11）。
