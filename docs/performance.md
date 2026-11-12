# Performance

## 処理基盤改修の比較（2026-11-12）

同じクラウド環境で、`95ee77729271f25aedd02a31bb06f1c1b48f5bee` と今回の
処理基盤改修のチェックポイント（`24d262a` / `295bb60`）を比較した。
以下の3列表はCSV追加最適化前の記録で、最終出力性能は後の追記で比較する。
Polars は両方とも **0.40.0** のままである。
過去の WSL2 計測とはマシンが異なるため、下の履歴の数値とは比較しない。

- Debian 13 container / Linux 6.18.57、AMD EPYC 9V45。論理CPUは5個見えるが
  cgroupのCPU割当は4個相当、メモリ上限は32GiB。Rust 1.99.1。
- release の共通設定は thin LTO、codegen-units=1、panic=abort、strip=true。
  `s` と `3` は、依存ライブラリも含む全体の `opt-level` の違い。
  構造変更だけの比較にはs同士を使い、旧版sと現行3はビルド設定も違うことに注意する。
- `gen_fixture.py --seed 42` で生成した同一ファイルを使用。7列100kは9.831MiB、
  7列2Mは197.175MiB、120列100kは108.341MiB。ハッシュは
  [manifest.json](bench/2026-11-12/manifest.json) に記録した。
- 全ビルドと他の重い検証が終わってから、baseline → 現行s → 現行3 の順で実行。
  各fixtureは別プロセス。各操作は **n=3 の中央値**、単位はms。
  p95や統計的な有意差は示さない。OSキャッシュは落としておらず、入力CSVは生成済み、
  Parquetは直前に取り込んだものなので、ディスクcoldの測定ではない。
- 再開の互換APIは両方とも250行を返す。アプリ用APIは旧版では同じ全列読込の
  `load_project`、現行では取込時に保存したプロファイルを読むmetadata専用経路。
  CSV出力は全行・注釈列付き・時刻降順。検索と出力は各試行前に結果キャッシュを空にし、
  検索結果の再表示と列集計は構築済みの同じ検索条件を使う。
- RSSは各fixtureの一連の操作を通した **engineプロセスの累積VmHWM**。
  操作単独のピークやWebViewを含むアプリ全体のメモリではない。

### 100k.csv

| 操作（ms、中央値） | 95ee777 / s | 現行 / s | 現行 / 3 |
|---|---:|---:|---:|
| 取込 | 328.42 | 283.67 | 223.43 |
| 再開（先頭250行を含む互換API） | 384.37 | 4.67 | 4.10 |
| 再開（アプリ用metadata API） | 420.57 | 0.67 | 0.63 |
| 先頭250行 | 1.70 | 2.56 | 2.11 |
| 中間250行 | 12.72 | 4.40 | 3.73 |
| 末尾250行 | 18.05 | 5.88 | 4.97 |
| 離れた4行（重複あり） | 18.13 | 16.77 | 15.01 |
| 列指定検索（初回） | 20.06 | 11.96 | 10.22 |
| 同じ検索結果の再表示 | 2.07 | 5.89 | 5.05 |
| 検索結果の列集計 | 41.09 | 7.07 | 6.46 |
| 全行を時刻の降順でCSV出力 | 69.65 | 161.37 | 140.77 |
| VmHWM（MiB） | 103.1 | 58.7 | 58.6 |

### 2m.csv

| 操作（ms、中央値） | 95ee777 / s | 現行 / s | 現行 / 3 |
|---|---:|---:|---:|
| 取込 | 3,801.79 | 4,981.09 | 3,803.02 |
| 再開（先頭250行を含む互換API） | 8,394.79 | 6.48 | 6.53 |
| 再開（アプリ用metadata API） | 7,620.03 | 1.88 | 1.41 |
| 先頭250行 | 3.13 | 3.16 | 3.32 |
| 中間250行 | 57.62 | 5.12 | 5.45 |
| 末尾250行 | 121.91 | 5.77 | 6.67 |
| 離れた4行（重複あり） | 220.34 | 16.76 | 15.65 |
| 列指定検索（初回） | 396.86 | 392.93 | 332.35 |
| 同じ検索結果の再表示 | 5.56 | 8.13 | 6.30 |
| 検索結果の列集計 | 864.25 | 133.04 | 121.92 |
| 全行を時刻の降順でCSV出力 | 1,765.05 | 7,053.15 | 6,483.64 |
| VmHWM（MiB） | 1,539.6 | 282.0 | 284.8 |

### long-100k.csv

| 操作（ms、中央値） | 95ee777 / s | 現行 / s | 現行 / 3 |
|---|---:|---:|---:|
| 取込 | 2,966.51 | 2,784.19 | 2,595.26 |
| 再開（先頭250行を含む互換API） | 8,315.42 | 39.33 | 36.38 |
| 再開（アプリ用metadata API） | 7,861.19 | 8.43 | 6.03 |
| 先頭250行 | 8.40 | 8.52 | 7.58 |
| 中間250行 | 53.96 | 8.63 | 7.07 |
| 末尾250行 | 108.73 | 17.89 | 16.62 |
| 離れた4行（重複あり） | 111.36 | 28.60 | 25.48 |
| 列指定検索（初回） | 62.32 | 60.33 | 51.32 |
| 同じ検索結果の再表示 | 38.08 | 40.78 | 33.48 |
| 検索結果の列集計 | 149.89 | 20.42 | 16.04 |
| 全行を時刻の降順でCSV出力 | 579.79 | 661.70 | 699.74 |
| VmHWM（MiB） | 694.1 | 81.4 | 83.0 |

### 実際に読んだParquetと、残る速度の課題

2M fixtureの現行opt3で、先頭・中間・末尾のページはそれぞれ **1 row group**
だけを読む。先頭250行のdecodeは250行、中間は11,237行、末尾は23,743行。
末尾へ移動するために先頭から2M行をdecodeする経路はなくなった。
離れた4行（末尾・先頭・中間・末尾）は3 groups / 48,319行で、元の順序と重複を保つ。
いずれも1 batchは最大8,192行。元行IDから組み立てた期待値を使う
`parquet_access_test`でも、5,199行と2M行、型・null・ナノ秒精度を検証した。

| 2M fixture / 現行3 | groups | decoded rows | 圧縮column bytes | readerが読んだbytes |
|---|---:|---:|---:|---:|
| 先頭250行 | 1 | 250 | 1,030,315 | 1,162,557 |
| 中間250行 | 1 | 11,237 | 967,074 | 1,099,316 |
| 末尾250行 | 1 | 23,743 | 628,734 | 760,976 |
| 離れた4行（重複あり） | 3 | 48,319 | 2,626,123 | 2,758,365 |
| 検索結果の列集計 | 55 | 2,000,000 | 1,512,253 | 1,644,495 |
| 全行を時刻の降順でCSV出力 | 110 | 4,000,000 | 23,448,636 | 23,713,120 |

このbytesはParquet readerの実際の`Read`と選択した圧縮column chunkの大きさで、
API呼出回数だけの指標ではない。ただしOSの物理ディスクI/O量ではなく、metadata専用の
別経路によるfooter読込や、CSV出力先・一時sort runのI/Oは含めない。
集計は対象列のみ1回、出力はsort列と出力列を別々に1回ずつ読み、全列DataFrameは作らない。
metadata再開ではデータ行をdecodeしない。旧案件は初回だけ列プロファイルをbatchで作り、
次回以降は保存済みキャッシュを使うため、移行初回の速度はこの表とは異なる。

再開・深いページ・集計とRSSには大きな改善がある。一方、**全行を並べ替えてCSV出力する
速度は悪化**した。2Mではs同士で1.77s→7.05s、現行3でも6.48s。
結果順を保ちながら行データを全件メモリに置かないための一時run書出・mergeが加わる。
これはチェックポイント時点の課題として記録し、後述の追加最適化で再測定した。
単調な逆順などの専用経路や一時I/O削減は、さらに改善できる候補である。
小さい案件の先頭表示やキャッシュ済み検索も一部遅く、全操作が速くなったわけではない。

速度を優先するなら **opt-level=3を推奨**する。2Mの取込は現行sの4.98sから3.80sへ戻り、
初回列検索は393ms→332ms、キャッシュ済み検索は8.13ms→6.30ms。
benchバイナリは14.76MiB→18.58MiB（約26%増）である。ただし120列の出力は
3の中央値がsより遅く、3試行の623–1,141msという幅もあるため、全経路への改善は保証しない。
この測定後、速度優先の方針に合わせてproduction releaseのopt-levelも3へ変更した。

### 追記: CSV並べ替え出力の追加最適化

上のチェックポイントで見つかった出力の遅さに対し、並べ替え順やbatch処理は変えず、
mergeの1レコードごとの汎用copyをbufferのsliceコピーへ変更した。
CSV recordの組立も1 byteずつのpushからsegment単位へ変え、completeなrecordは
中間bufferへ複製せずにsorterへ渡す。引用符や改行がwrite境界をまたぐケースは
すべてのchunkサイズでの回帰テストを通した。

sourceのdecode batch上限は **8,192行のまま**。sort runは **65,536 entriesかつ8MiB payload**
の両方で制限し、entries自体は最大1.5MiB、mergeは32-wayでheaderだけをheapに保持する。
巨大な単独recordの専用runと、rank indexの32MiB budgetも維持する。
行データを全件DataFrameへ戻す変更ではない。

同じopt3の変更前binaryと変更後binaryを保存し、他のビルド・重い検証が止まった状態で
全く同じfixture / 一連の操作を再実行した。CSV出力だけを下表で比較する。
いずれもn=3、RSSは操作単体ではなく一連の操作の累積VmHWMである。

| fixture | 変更前中央値（ms） | 変更後中央値（ms） | 変更後min–max（ms） | 変更前RSS（MiB） | 変更後RSS（MiB） |
|---|---:|---:|---:|---:|---:|
| 100k.csv | 178.81 | 54.05 | 52.95–55.08 | 59.2 | 57.9 |
| 2m.csv | 6,150.73 | 1,990.19 | 1,318.23–2,775.06 | 284.2 | 283.7 |
| long-100k.csv | 756.76 | 715.25 | 588.55–1,032.98 | 79.0 | 71.6 |

2Mの中央値は6.15s→1.99s（約68%短縮）。初回の改修時点で生じた大きな遅さは抑えたが、
旧版sの1.77sに対しては依然約13%長い。120列も約715msで、旧版の約580msより長い。
3試行の幅が大きいため、残る差を解消した・常に高速化したとは扱わない。
一方、このworkflowの2Mピークは約284MiBで、旧版の約1,540MiBを大きく下回る。
source readerのgroups / decoded rows / bytesは変更前後で一致しており、
条件や出力対象を減らして得た差ではない。追加改善は一時I/Oと順序復元の費用が対象になる。

追加最適化の全試行: [変更前opt3](bench/2026-11-12/before-export-fix.jsonl)、
[変更後opt3](bench/2026-11-12/after-export-fix.jsonl)。初回比較のraw値は書き換えていない。

### 再現方法と全試行

既存の`95ee777` detached worktreeを用意し、リポジトリルートから次を実行する。
ベースラインには共通benchと、計測APIだけを旧版へ合わせるshimを追加し、
production engineのソースは変更しない。出力には各試行の時間・累積RSS・読込counterを残す。

```sh
scripts/bench/run_comparison.sh /absolute/path/to/baseline-worktree /tmp/trivium-perf
python3 scripts/bench/summarize.py /tmp/trivium-perf/baseline-opt-s.log /tmp/trivium-perf/current-opt-s.log /tmp/trivium-perf/current-opt-3.log
```

全試行: [baseline s](bench/2026-11-12/baseline-opt-s.jsonl)、
[現行s](bench/2026-11-12/current-opt-s.jsonl)、[現行3](bench/2026-11-12/current-opt-3.jsonl)。
将来のデータ・CPU・ディスクによる差、バッチ実行中のキャンセル応答時間、WebView込みRSS、
フロントの表示時間やページキャッシュ保持量はこのbenchmarkの対象外である。

## 履歴: T27 実測（2026-11-11、engine）

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
| import cold（n=3、median） | 206.2ms | 921.6ms | 4330.5ms | 2382.5ms |
| import cold（n=3、min–max） | 205.7–239.1ms | 901.6–937.1ms | 4019.8–5504.0ms | 2350.3–2419.4ms |
| 案件再開 p50（n=20） | 380.3ms | 1461.7ms | 7411.1ms | 6978.6ms |
| 案件再開 p95（n=20） | 397.1ms | 1495.2ms | 7749.3ms | 7220.2ms |

import は同じプロセスでの繰り返しではなく、fixture ごとに別プロセスで
3 回実行した値である。OS キャッシュが温まった状態の import は分離して
測っていない。案件再開は `ProjectsStore` の作り直しと `load_project` を
含むが、20 回を 1 プロセスで連続実行しており、初回 cold と 2 回目以降の
warm を分けていない。1 回の `load_project` が全列 DataFrame を読むため、
2M では 7 秒台になる。

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
| 案件削除後のキャッシュ | 1→0 / 0MiB | 1→0 / 0MiB | 1→0 / 0MiB | 1→0 / 0MiB |
| import 後 VmHWM | 84.3MiB | 89.9MiB | 101.9MiB | 97.1MiB |
| 再開後 VmHWM | 104.1MiB | 231.4MiB | 973.0MiB | 654.5MiB |
| 検索後 VmHWM | 104.1MiB | 231.4MiB | 973.0MiB | 654.5MiB |
| 出力後 VmHWM | 121.2MiB | 270.5MiB | 1188.2MiB | 679.9MiB |
| 削除前 VmHWM | 121.2MiB | 277.6MiB | 1226.4MiB | 687.0MiB |
| 削除後 VmHWM / VmRSS | 121.2 / 90.1MiB | 277.6 / 231.5MiB | 1226.4 / 927.8MiB | 687.0 / 573.4MiB |

- 2M の結果IDキャッシュは 2,000,000 行分の ID で 15.259MiB。64MiB 上限内。
- 案件削除で結果IDキャッシュのエントリとバイトは 0 になった。案件を
  「閉じる」だけの経路（画面から別案件へ切り替える／プロジェクトを閉じる）
  での解放は測っていない。削除は別経路であり、close の代用にはしない。
- ただし `VmHWM` はプロセスの最大値なので削除後も下がらない。`VmRSS` も
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
