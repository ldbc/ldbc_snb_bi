import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.summary.*;

public class Main implements AutoCloseable
{
    private final Driver driver;

    public Main( String uri )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.none() );
    }

    @Override
    public void close()
    {
        driver.close();
    }

    public QuerySummary executeQuery( String querySpec, Map<String,Object> queryParams, boolean stats )
    {
        try ( Session session = driver.session( SessionConfig.forDatabase( "neo4j" ) ) )
        {
            return session.writeTransaction( tx ->
            {

                Result result = tx.run( querySpec, queryParams );

                if ( stats )
                {
                    System.out.println( "Results: " + result.list() );
                }

                ResultSummary resultSummary = result.consume();

                long hits = 0L;
                long records = 0L;
                for ( ProfiledPlan profiledPlan :
                        resultSummary.profile().children() )
                {
                    records = records + profiledPlan.records();
                    hits = hits + profiledPlan.dbHits();
                    records = records + getRecords( profiledPlan );
                    hits = hits + getHits( profiledPlan );
                }

                return new QuerySummary( records, hits );
            } );
        }
    }

    public static long getHits( ProfiledPlan profiledPlan )
    {
        long hits = profiledPlan.dbHits();
        for ( ProfiledPlan plan : profiledPlan.children()
        )
        {
            hits = hits + getHits( plan );
        }

        return hits;
    }

    public static long getRecords( ProfiledPlan profiledPlan )
    {
        long records = profiledPlan.records();
        for ( ProfiledPlan plan : profiledPlan.children()
        )
        {
            records = records + getRecords( plan );
        }

        return records;
    }

    public static String getQuerySpec( String biDir, String queryNum ) throws IOException
    {
        queryNum = queryNum.replace( "a", "" );
        queryNum = queryNum.replace( "b", "" );

        var fileName = Path.of( biDir + "/cypher/queries/bi-" + queryNum + ".cypher" );
        var actual = Files.readString( fileName );
        var split = Arrays.asList( actual.split( "\\*/", 2 ) );

        var nonParallelRuntime = new HashSet<>( Arrays.asList( "2", "4", "10", "13", "15", "16", "17", "18", "19", "20" ) );

        String result;
        if ( nonParallelRuntime.contains( queryNum ) )
        {
            result = "PROFILE" + split.get( 1 );
        }
        else
        {
            result = "PROFILE CYPHER runtime=parallel " + split.get( 1 );
        }
        return result;
    }

    public static List<String[]> getQueryParams( String biDir, String queryNum ) throws IOException
    {
        String fileName = biDir + "/parameters/bi-" + queryNum + ".csv";
        List<String[]> params = new ArrayList<>();
        var csvFile = new File( fileName );
        if ( csvFile.isFile() )
        {
            var csvReader = new BufferedReader( new FileReader( csvFile ) );

            String row;
            while ( (row = csvReader.readLine()) != null )
            {
                String[] data = row.split( "\\|" );
                params.add( data );
            }
        }

        return params;
    }

    public static Object convert( String type, String value )
    {

        Object result = null;
        if ( Objects.equals( type, "STRING" ) )
        {
            result = value;
        }
        else if ( Objects.equals( type, "INT" ) )
        {
            result = Integer.parseInt( value );
        }
        else if ( Objects.equals( type, "ID" ) )
        {
            result = Long.parseLong( value );
        }
        else if ( Objects.equals( type, "DATETIME" ) )
        {
            var formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'" );
            var date = LocalDateTime.parse( value, formatter );
            var zoneId = ZoneId.of( "Etc/UTC" );
            return date.atZone( zoneId );
        }
        else if ( Objects.equals( type, "DATE" ) )
        {
            var formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd" );
            var date = LocalDate.parse( value, formatter );
            var zoneId = ZoneId.of( "Etc/UTC" );
            return date.atStartOfDay( zoneId );
        }
        return result;
    }

    public static void main( String... args ) throws Exception
    {
        var q = args[0];
        var executions = Long.parseLong( args[1] );
        var stats = Boolean.parseBoolean( args[2] );

        String[] queries;
        if ( q.equals( "all" ) )
        {
            queries = new String[]{"1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a",
                                   "16b", "17", "18", "19a", "19b", "20"};
        }
        else
        {
            queries = new String[]{q};
        }

        // Get BI directory
        var biDir = System.getenv( "LDBC_SNB_BI" );
        System.out.println( "LDBC_SNB_BI: " + biDir );

        try ( Main conn = new Main( "neo4j://localhost:7687" ) )
        {
            for ( var query : queries
            )
            {
                System.out.println( "Executing query " + query );

                // query spec
                var querySpec = getQuerySpec( biDir, query );
                if ( stats )
                {
                    System.out.println( "Query specification: " + querySpec );
                }

                // query params
                var queryParams = getQueryParams( biDir, query );

                // headers
                var headers = queryParams.remove( 0 );
                List<String> types = new ArrayList<>();
                List<String> names = new ArrayList<>();
                for ( var param : headers
                )
                {
                    String[] nameAndType = param.split( ":" );
                    names.add( nameAndType[0] );
                    types.add( nameAndType[1] );
                }

                // create output file
                var outFileName = "./data/bi-" + query + "-summary.csv";
                FileWriter csvWriter = new FileWriter( outFileName );
                csvWriter.append( String.join( "|", "dbHits", "records", "runtime", "parameters" ) );
                csvWriter.append( "\n" );

                int executed = 0;

                for ( String[] param : queryParams
                )
                {
                    Map<String,Object> params = new HashMap<>();

                    for ( int j = 0; j < param.length; j++ )
                    {
                        var paramName = names.get( j );
                        var paramValue = convert( types.get( j ), param[j] );
                        params.put( paramName, paramValue );
                    }

                    if ( stats )
                    {
                        System.out.println( "Parameters: " + params );
                    }

                    var start = System.currentTimeMillis();
                    var summary = conn.executeQuery( querySpec, params, stats );
                    var finish = System.currentTimeMillis();
                    var timeElapsed = (finish - start);

                    if ( stats )
                    {
                        System.out.println( "Time elapsed (ms): " + timeElapsed );
                        System.out.println( "DB Hits: " + summary.getHits() );
                        System.out.println( "Records: " + summary.getRecords() );
                    }
                    else
                    {
                        System.out.print( "Executed: " + executed + "\r" );
                    }

                    csvWriter.append(
                            String.join( "|",
                                    Long.toString( summary.getHits() ),
                                    Long.toString( summary.getRecords() ),
                                    Long.toString( timeElapsed ),
                                    params.toString() ) );
                    csvWriter.append( "\n" );
                    executed++;

                    if ( executed == executions )
                    {
                        break;
                    }
                }
                System.out.println();
                System.out.println( "Output saved to " + outFileName );
                csvWriter.flush();
                csvWriter.close();
            }
        }
    }
}
