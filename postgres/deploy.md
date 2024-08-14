## Deployment Notes

- Create database instance
- Update configuration in `db.env`
- Ingest parquet file with `load_to_prod.sh` (may require `chmod +x load_to_prod.sh`)
- Create index on hex_id (for performance):`CREATE INDEX idx_hex_id ON space2stats (hex_id)` - critical for performance of our queries
- Test with the [example notebook](notebooks/space2stats_api_demo.ipynb)