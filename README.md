# SoccerFieldMap 数据抓取器

脚本会从 https://www.soccerfieldmap.com 抓取全部场地详情页链接，优先模拟“Explore 页 -> STATE -> FIELDS”的点击流程（直接解析 Explore 页的 Next.js 数据），若拿不到则回退到 sitemap 和 API，再逐个打开详情页解析场地名，并用场地名去维基百科查询场地面积。结果实时写入 CSV，方便中途查看或恢复。

## 输出字段
CSV 列名（中文）：
1. 场地名
2. 场地链接（soccerfieldmap 站内的详情页链接）
3. 场地面积（平方米，查不到留空）
4. 场地面积 wiki 链接（查不到留空）

## 使用方法
```bash
python scraper.py \
  --csv data/fields.csv \
  --progress data/progress.json \
  --wiki-workers 8 \
  --log-level INFO
```

- 运行过程中每写入一行都会立刻 `fsync`，CSV 随时可查看。
- `progress.json` 保存已经处理过的场地链接；再次运行会跳过已保存的链接，支持中断后继续。
- 如果需要加快或放缓请求，可调整：
  - `--wiki-workers`（并发维基请求数）
  - `--max-attempts`、`--backoff`、`--backoff-cap`、`--timeout`（HTTP 重试和超时）

## 依赖安装
```bash
pip install -r requirements.txt
```

## 快速验证
```bash
python -m compileall scraper.py
```
