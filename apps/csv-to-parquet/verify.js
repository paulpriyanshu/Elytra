const { Database } = require('duckdb-async');
async function verify() {
    const db = await Database.create(":memory:");
    const rowGroups = await db.all("SELECT * FROM parquet_metadata('/Users/priyanshupaul/Downloads/archive/custom_1988_2020.parquet')");
    console.log(`Total Row Groups: ${rowGroups.length}`);
    if (rowGroups.length > 0) {
        console.log('Sample Row Group Metadata:', JSON.stringify(rowGroups[0], (key, value) =>
            typeof value === 'bigint' ? value.toString() : value
            , 2));
    }
}
verify();
