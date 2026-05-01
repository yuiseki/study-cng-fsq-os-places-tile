# study-cng-fsq-os-places-tile

> **A Cloud Native Geospatial study, sibling of [study-cng-overture-buildings-tile][prev]: dynamic vector tiles served on the fly from Foursquare OS Places GeoParquet via DuckDB Spatial + a Parquet `_metadata`-driven spatial index + Knative.** Same infra as the Overture study, different dataset and different "which file do I read?" mechanism.

| | |
| --- | --- |
| **viewer (static)** | https://yuiseki.github.io/study-cng-fsq-os-places-tile/ |
| **function (serverless)** | https://fsq-places-cng.yuiseki.com/tiles/{z}/{x}/{y}.mvt |
| **example URL** | https://fsq-places-cng.yuiseki.com/tiles/15/9647/12320.mvt?filter_by_category=Dining%20and%20Drinking |

## なぜ作ったか

[`study-cng-overture-buildings-tile`][prev] で「TiTiler のベクター版」を Overture Buildings で組んだ。同じ構成のまま **dataset を差し替えると、 何が変わって何が変わらないか** を体感するのがこの study の目的。具体的には:

- **変わらない**: FastAPI + DuckDB Spatial + httpfs + Knative ksvc + MapLibre + Cloudflare Tunnel という骨格
- **変わる**: 「どの Parquet ファイルを読むか」 を判定する仕組み

Overture では公式が **STAC catalog** (`https://stac.overturemaps.org/`) を別レイヤーとして公開していて、 起動時に collection.json + 512 個の item.json を並列 fetch して `(bbox, s3_href)` インデックスを作る。 一方、 Foursquare OS Places は STAC を持っていないが、 [Source Cooperative の Fused 配信版][fused-fsq] が **Parquet 標準の `_metadata` ファイル** (~20 MB) をデータと一緒に同梱している。 これを pyarrow で 1 回 fetch すれば、 全 row group の `file_path` と `bbox` 列の stats から同じ `(bbox, s3_href)` インデックスが組める。

つまり「STAC catalog」 vs 「Parquet `_metadata`」 は **どちらも file 選定のための out-of-band / in-band メタデータ** であり、 配置形態が違うだけで CNG primitive としての役割は等価、 という発見。

[prev]: https://github.com/yuiseki/study-cng-overture-buildings-tile
[fused-fsq]: https://source.coop/fused/fsq-os-places

## できること

- ブラウザでパン・ズームすると、 その bbox に該当する Foursquare OS Places の点 POI が **動的に** 取得される（事前のタイルセット build なし）
- ドロップダウンで **`filter_by_category=Dining and Drinking`** のようなクエリパラメータが切り替わり、 サーバ側で違う SQL が走り、 即座に結果が変わる
- 同じインフラで `filter_by_category` を `Travel and Transportation`、 `Health and Medicine` などに切り替えるのは UI 1 操作
- viewer は GitHub Pages、 function は Knative ksvc というフロント／バックのクリーンな分離

## アーキテクチャ

```
┌──────────────────────────────────────────┐         ┌─────────────────────────────────┐
│ frontend (static)                        │  HTTPS  │ function (dynamic)              │
│ yuiseki.github.io/                       │ ──────► │ fsq-places-cng.yuiseki.com          │
│ study-cng-fsq-os-places-tile/            │         │ (Knative ksvc on z-t)           │
│   docs/index.html  (MapLibre GL JS)      │         │   FastAPI (uvicorn)             │
│   docs/style.json  (Esri World Imagery)  │         │     ↓                           │
└──────────────────────────────────────────┘         │   Parquet _metadata index       │
                                                     │   (in-memory, built at startup) │
                                                     │     ↓                           │
                                                     │   DuckDB Spatial + httpfs       │
                                                     │     ↓                           │
                                                     │   s3://us-west-2.opendata       │
                                                     │     .source.coop/fused/         │
                                                     │     fsq-os-places/.../places/   │
                                                     └─────────────────────────────────┘
```

### 1 リクエストの流れ

1. ブラウザが `https://fsq-places-cng.yuiseki.com/tiles/15/9647/12320.mvt?filter_by_category=Dining and Drinking` に GET
2. Cloudflare Tunnel が z-t の Knative Kourier 入口（NodePort）に転送
3. Knative が ksvc（pod 数 0 ならここで cold start）にルーティング
4. FastAPI ハンドラが `(z, x, y)` から bbox を計算
5. **Parquet `_metadata` index** から bbox に該当する Parquet ファイル（81 個中の 1〜数個）の `s3://` URL を取得
6. **DuckDB Spatial** がその数個のファイルだけを `read_parquet([file1, file2, ...])` で開き、 row group prune + `bbox` struct WHERE 句 + `EXISTS / UNNEST(fsq_category_labels)` で絞り込み
7. 結果を WKB で取得、 [`mapbox-vector-tile`][mvt] で MVT bytes にエンコード
8. レスポンス

[mvt]: https://github.com/tilezen/mapbox-vector-tile

### Cold start の見積もり

| ステージ | 時間 | 内訳 |
| --- | ---: | --- |
| コンテナ起動 | ~5 秒 | Knative pod スケジューリング |
| DuckDB extension load | ~3 秒 | `spatial` + `httpfs` の初回 install |
| `_metadata` fetch + parse | ~6 秒 | 20 MB を 1 回ダウンロード、 pyarrow で 5646 row groups を集約 |
| `_sample` taxonomy build | ~2 秒 | 10 top labels + 625 leaf ids の dict 化 |
| 1 タイル目の query | ~5 秒 | 1 ファイル目の Parquet metadata fetch + row group read |
| **合計** | **~21 秒** | これ以降はキャッシュが効いて 0.5〜1 秒 / タイル |

scale-to-zero された後、 1 個目のリクエストでこれが走る。 2 個目以降は ksvc が pod を保持している間（idle まで 60 秒）はホットで叩ける。

## Overture 版との差分（study の核心）

| 観点 | [study-cng-overture-buildings-tile][prev] | study-cng-fsq-os-places-tile (この study) |
| --- | --- | --- |
| 配信元 | `s3://overturemaps-us-west-2/` (公式) | `s3://us-west-2.opendata.source.coop/fused/` (Fused が再 partition) |
| ファイル数 / サイズ | 512 / 276 GB | 81 / 15.7 GB |
| Hive partitioning | `release=/theme=/type=` | なし (places/N.parquet 連番) |
| Spatial partitioning | なし (ID 順、 Manhattan が散らばる) | あり (Fused が地理空間で分割) |
| 「どの file を読むか」 | **STAC catalog** (HTTP, out-of-band JSON) | **`_metadata`** (in-band Parquet) |
| index 構築コスト | ~7 秒 (collection.json + 512 item.json 並列 fetch) | ~7 秒 (20 MB の `_metadata` を 1 回 fetch) |
| geometry 列 | `geometry` (WKB) | `geometry` (DuckDB GEOMETRY('EPSG:4326')) |
| bbox 列 | あり | あり (struct) |
| 主フィルタ軸 | `height` (連続値) | `fsq_category_labels` (breadcrumb prefix) |
| 描画 | `fill-extrusion` (3D) | `circle` (point) |

学び:

- **GeoParquet** は「file 内の効率（row-group prune）」を保証するが、 「どの file を読むか」 は規定しない
- **Hive partitioning** は directory 命名で prune するが、 spatial 軸では効かない（producer の partitioning 戦略次第）
- 「どの file を読むか」 は **out-of-band（STAC catalog）** にも **in-band（Parquet `_metadata`）** にも置ける。 同じ役割の primitive が、 配信形態として 2 通り存在する
- **dataset そのものが地理空間 partitioning されているか** どうかも独立軸。 Fused の Foursquare 版は「ID 順」 ではなく明示的に地理空間で分割している（`0.parquet` が西半球南側、 `80.parquet` が極東〜太平洋）

## フィルタ列の stats 非対称性とその解決

`filter_by_height` (Overture, double 型) と `filter_by_category` (Foursquare, VARCHAR[]) の間には、 当初 cold-start を超えるほど大きな性能差があった。 配列列に row group 統計が無いため、 `EXISTS (SELECT 1 FROM UNNEST(fsq_category_labels) WHERE label LIKE prefix || '%')` は bbox で絞った row group の **全行を seq scan** することになる。 これが「**GeoParquet において bbox は特権的なフィルタ列だが、 配列列にはその恩恵がない**」 という Foursquare 版で初めて顕在化した非対称性。

解決策として、 **breadcrumb prefix LIKE を leaf id 集合の equality match に置き換えた**。 Foursquare の `fsq_category_ids` には階層親 id ではなく **leaf id のみ** が入るので、 単純な `array_has(top_id)` では引けない。 そこで起動時に dataset の `_sample` part (~2 MB) を 1 回スキャンして `top_label -> [leaf_id, ...]` の taxonomy を build し、 リクエスト時に `Dining and Drinking` を 116 個の leaf id 集合に展開して `array_has_any(fsq_category_ids, [...])` で filter する設計にしている。

Manhattan z=15 の単一タイルで実測した改善:

| クエリ | 切り替え前 (LIKE / UNNEST) | 切り替え後 (`array_has_any`) | 倍率 |
| --- | ---: | ---: | ---: |
| no filter | ~4.4 秒 | ~4.2 秒 | 〜 |
| `filter_by_category=Dining and Drinking` (cold) | ~250 秒 | **~4.6 秒** | **54×** |
| 同 (warm 2 回目) | ~80 秒 | **~0.55 秒** | **147×** |
| `filter_by_category=Retail` (別 category, warm) | -- | ~0.51 秒 | -- |

なぜ効いたか:

- DuckDB は VARCHAR 列の **dictionary encoding** を活用し、 array element の equality を整数 id 比較に落とせる
- LIKE / UNNEST の per-row 評価が消え、 `array_has_any` 1 回の集合演算になる
- 同じファイルへの category 違いクエリは S3 cache が効いて 0.5 秒台に収まる

トレードオフ:

- breadcrumb 細粒度フィルタ (`Dining and Drinking > Restaurant > Sushi Restaurant`) は今回の API では諦めた。 Foursquare の `fsq_category_ids` が leaf-only なので、 sub-tree フィルタは別の taxonomy expander を用意して leaf 集合に変換する必要があり、 study のスコープ外
- `_sample` (2 MB) に出現しない leaf id は taxonomy に載らないため、 ロングテール category は filter できない（unknown label は空結果になる）。 完全網羅したい場合は全 81 ファイルから DISTINCT を集める必要があるが、 cold start が数十秒伸びるので採用しなかった

学び: 「**配列列のフィルタは equality 集合に落とせ**」 が CNG 上の vector dataset を扱うときの実用ルール。 `prefix LIKE` のような「人間に直感的なクエリ」 を裏で集合 equality に変換するのは、 ベクター CNG では普遍的に出てくるパターンになる。

## 動かす

### ローカル開発

```bash
# function サーバ（DuckDB + FastAPI、 port 8007）
uv sync
uv run python -m fsq_os_places_cng.server

# 別ターミナルで viewer 起動
cd docs && python3 -m http.server 8000
# ブラウザで http://localhost:8000/?server=http://localhost:8007 を開く
```

### コンテナ build + Knative deploy（z-t）

```bash
docker build -t fsq-places-cng:0.1.0 -f docker/Dockerfile .
docker save fsq-places-cng:0.1.0 -o fsq-places-cng-0.1.0.tar
# z-t に転送 → containerd に import → kubectl apply
ctr -n=k8s.io images import fsq-places-cng-0.1.0.tar
kubectl apply -f k8s/ksvc.yaml
```

## ファイル構造

```
study-cng-fsq-os-places-tile/
├── README.md
├── LICENSE.md
├── pyproject.toml          # Python project (uv)
├── .python-version         # 3.12
├── src/
│   └── fsq_os_places_cng/
│       ├── __init__.py
│       ├── server.py             # FastAPI app + /health + /categories + /tiles/{z}/{x}/{y}.mvt
│       ├── parquet_index.py      # _metadata-driven (bbox -> s3) index
│       ├── category_taxonomy.py  # _sample-driven (top_label -> leaf_ids) index
│       ├── duckdb_query.py       # DuckDB Spatial query layer
│       ├── filters.py            # query parameter parsing
│       └── mvt.py                # WKB -> MVT encoding
├── tests/
│   ├── test_filters.py
│   └── test_parquet_index.py
├── docker/
│   └── Dockerfile
├── k8s/
│   └── ksvc.yaml           # Knative Service manifest
└── docs/                   # GitHub Pages root
    ├── index.html          # MapLibre viewer
    └── style.json          # Esri World Imagery basemap
```

## License

MIT for this repository's source. Foursquare OS Places data itself is Apache 2.0 (Copyright Foursquare Labs, Inc.); the geospatially repartitioned distribution is published by Fused.io on Source Cooperative under the same license. See [LICENSE.md](LICENSE.md).
