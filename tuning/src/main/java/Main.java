import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

    public QuerySummary executeQuery( String querySpec, Map<String,Object> queryParams )
    {
        try ( Session session = driver.session( SessionConfig.forDatabase( "neo4j" ) ) )
        {
            return session.writeTransaction( tx ->
            {

                Result result = tx.run( querySpec, queryParams );

                //System.out.println(result.list());

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


//                System.out.println( "Profiled: " + resultSummary.hasProfile() );
//                System.out.println( "Query plan: " + resultSummary.profile().toString() );
//                System.out.println( "Records: " + records );
//                System.out.println( "DB Hits: " + hits);

                return new QuerySummary( records, hits );
//                return new QuerySummary( 0, 0 );
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
        // TODO: hack
        queryNum = queryNum.replace( "a", "" );
        queryNum = queryNum.replace( "b", "" );

        Path fileName = Path.of( biDir + "/cypher/queries/bi-" + queryNum + ".cypher" );
        String actual = Files.readString( fileName );
        List<String> split = Arrays.asList( actual.split( "\\*/", 2 ) );

        return "PROFILE " + split.get( 1 );
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
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'" );
            result = LocalDateTime.parse( value, formatter );
        }
        else if ( Objects.equals( type, "DATE" ) )
        {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd" );
            result = LocalDate.parse( value, formatter );
        }
        return result;
    }

    public static void main( String... args ) throws Exception
    {
//        // Queries with variants
//        String[] queries =
//                {"1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17",
//                 "18", "19a", "19b", "20"};
        String[] queries =
                {"7"};

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
                System.out.println( "Query specification: " + querySpec );

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
                csvWriter.append( String.join( ",", "dbHits", "records" ) );
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

                    System.out.println(params);

                    var summary = conn.executeQuery( querySpec, params );

                    csvWriter.append( String.join( ",", Long.toString( summary.getHits() ), Long.toString( summary.getRecords() ) ) );
                    csvWriter.append( "\n" );
                    executed++;

                    //System.out.print( "executed:" + executed + "\r" );

                    if ( executed == 400 )
                    {
                        break;
                    }
                }

                System.out.println( "Output saved to " + outFileName );
                csvWriter.flush();
                csvWriter.close();
            }
        }
    }
}
