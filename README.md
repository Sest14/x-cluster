# x-cluster

X 上の言説と、その発言をしているアカウントを、集団レベルでクラスタ分析するプロジェクト。

- 方法論（必要な知識、統計の出し方、統計のマジック対策）：[docs/methodology.md](docs/methodology.md)
- 分析計画書（事前登録）：[docs/analysis-plan.md](docs/analysis-plan.md)
- 意思決定の記録（ADR）：[docs/adr/](docs/adr/)

---

## 収集スクリプト（xc.py）

起点の投稿を公式 X API で集め、抽出したアカウントのふだんの投稿などを TwexAPI のアカウント指定の検索（from:）で集めて、このフォルダ内の SQLite に保存します。分析用に仮名化したファイル（analysis.sqlite）を書き出すところまでを担当します（ADR-0015）。

## 必要なもの
- Python 3.9 以上（標準ライブラリのみで動くので、追加インストールは不要）
- このフォルダの `.env` に次の2行
  ```
  X_BEARER_TOKEN=公式 X API の Bearer Token
  TWEX_API_KEY=TwexAPI の APIキー
  ```
  `pdiag` を使う場合は、試すサービスのキーも追加します（下記）。

## 実行順
コマンドプロンプトでこのフォルダに移動してから実行します。ふだんは `run` だけで足ります。
```
py xc.py run         … 収集一式（count → search → sample → users → tbaseline → tweights → tany → export）
```

個別に実行する場合はこちら。
```
py xc.py check       … 公式APIのトークン確認（約$0.005）
py xc.py tcheck      … TwexAPI のキーと時間指定の確認
py xc.py tdiag ...   … TwexAPI の網羅性の診断（tdiag from 10 / tdiag fromid / tdiag regista 2026-11-05 [10min]）
py xc.py pdiag ...   … 他社のキーワード検索の網羅性の診断（下記）
py xc.py count       … 公式API：検索条件ごとの日別件数
py xc.py search      … 公式API：起点の投稿を日別比例で集める
py xc.py sample      … 起点の投稿者から各群のアカウントを無作為抽出
py xc.py users       … 抽出したアカウントのプロフィールと@名（resolve_via に応じて TwexAPI か公式API）
py xc.py tbaseline   … TwexAPI：炎上前の投稿（なければ直近の投稿）
py xc.py tweights    … TwexAPI：調査期間中の該当投稿数（抽出確率の重み）
py xc.py tany        … TwexAPI：touken_fans と対照群が炎上に言及したか（H3）
py xc.py export      … analysis.sqlite（仮名化済み）を作成
py xc.py status      … 現在の件数、使用クレジット、公式APIの推定使用額
```

どのコマンドも途中で止めて（Ctrl+C）、同じコマンドを再実行すれば続きから再開します。
TwexAPI の使用クレジットが `twex_max_credits`、公式APIの推定使用額が `budget_usd` を超えそうになると自動で止まります。

## 他社サービスの診断（pdiag）
公式APIの1時間ごとの件数（UTC 0〜6時、約$0.01。1回取れば以後は使い回し）と、各サービスのキーワード検索で取れた件数を比べます。
サービスごとに別のコマンドプロンプトで同時に実行できます。保存するのは投稿IDだけです。
```
py xc.py pdiag socialdata regista 2026-11-05
py xc.py pdiag twitterapi_io regista 2026-11-05
py xc.py pdiag scrapebadger regista 2026-11-05
py xc.py pdiag apify regista 2026-11-05
py xc.py pdiag twex regista 2026-11-05
py xc.py pdiag compare regista 2026-11-05   … 取得率、サービス間の重なり、和集合
```
末尾に `noreply`（返信以外）・`reply`（返信だけ）・`split`（両方を続けて実行して比べる）を付けると、返信の有無で分けて診断できます（例: `py xc.py pdiag twex regista 2026-11-05 split`）。
`py xc.py pdiag who regista 2026-11-05` で、見えない投稿がアカウント単位か投稿単位かを調べます（公式APIで投稿IDと投稿者IDだけを取得、約$2）。
| サービス | .env のキー名 | 間隔の初期値 |
|---|---|---|
| socialdata | `SOCIALDATA_API_KEY` | 21秒（無料枠: 毎分3回） |
| twitterapi_io | `TWITTERAPI_IO_KEY` | 5.5秒 |
| scrapebadger | `SCRAPEBADGER_API_KEY` | 1秒 |
| apify | `APIFY_TOKEN` | 1枠1回（アクターは `apify_actor`、初期値 `jacquemus~x-tweet-scraper`） |
| twex | `TWEX_API_KEY` | `twex_qps` に従う |

間隔は config.json の `pdiag_interval`（例: `{"socialdata": 21}`）で変えられます。

## ログ
どのコマンドも、画面の出力を `logs/<日時>_<コマンド>.log` にも保存します。

## 設定（config.json）
- `queries`：検索条件。`name` はラベル、`query` は公式APIの検索演算子で書く（TwexAPI 用には自動で書き換える）。`max_seed_posts` は起点の投稿の数（日別比例）、`n_accounts` は抽出する人数
- `start_time` / `end_time`：調査期間（`end_time` が `null` なら収集時点まで）
- `baseline_start` / `baseline_end`：「ふだん」を測る炎上前の期間。`baseline_max_posts` は1人あたりの取得上限、`recent_max_posts` は炎上前の投稿がない人の取得上限
- `daily_words`：日常の話を見分けるための語
- `phase1_rule`：運用の型の判定ルール（事前登録。分析を始めたら変えない）
- `label_sample_per_group`：Claude が詳しく判定するアカウントの数（各群）
- `any_controversy_query` / `any_controversy_groups`：H3 の言及確認に使う検索条件と対象の群
- `twex_qps`：TwexAPI への1秒あたりのリクエスト数（プランに合わせる。無料枠は0.2、Mini は1）
- `twex_time_format`：時間の範囲の書き方（`unix` か `date_utc`。`tcheck` の結果で選ぶ）
- `twex_max_credits`：TwexAPI のクレジット使用の上限
- `resolve_via`：プロフィールの取り方（`official` か `twex`。`tdiag fromid` の結果で選ぶ）
- `weight_count_cap`：重み用に数える投稿数の上限
- `budget_usd`：公式APIの推定使用額の上限

## ファイル
- `raw.sqlite`：取得したそのままのデータ。**このPCの外に出さないでください**
- `salt.secret`：仮名化に使う秘密鍵。**このPCの外に出さないでください**
- `analysis.sqlite`：アカウントID・@名・URLを仮名化したデータ。分析にはこれを使います

## データの保持
TwexAPI のデータは、X 上での削除への追従ができません。分析が終わったら `py xc.py purge --yes` で生データを消してください。集計結果は残ります。
