public class QuerySummary
{

    private final long records;
    private final long hits;

    public QuerySummary( long records, long hits )
    {
        this.records = records;
        this.hits = hits;
    }

    public long getRecords()
    {
        return records;
    }

    public long getHits()
    {
        return hits;
    }
}
