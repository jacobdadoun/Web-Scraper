package webscrapehw;

import org.jsoup.Jsoup; // A Library that allows you to connect to the web
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Email REGEX: \b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b
// Link REGEX: https?://(www\.)?[A-Za-z0-9]+\.(com|org|edu|gov|us)/?.*

public class WorkerThread implements Runnable {

    // emails and links must be stored in a thread safe container
    static private List<String> linksToVisit = Collections.synchronizedList(new ArrayList<>());
    static private List<String> linksVisited = Collections.synchronizedList(new ArrayList<>());
    static private List<String> emailSet = Collections.synchronizedList(new ArrayList<>());
    static private List<String> bannedLinks = Collections.synchronizedList((new ArrayList<>()));
    static private Map<String, Integer> checkDomainROI = Collections.synchronizedMap(new HashMap<>());
    //ConnectionURL will tell the java program how to connect to the DB
    private static final int EMAIL_MAX_COUNT = 10_000;
    private static int lastUploadCount;
    private String url;


    private WorkerThread(String sourceURL){
        url = sourceURL;
        run();
    }



    @Override
    public void run() {
        try {

            // 1. Check if the url hasn't caused us any problems in the past.
            if(!bannedLinks.contains(url)){
                URI uri = new URI(url);
                String siteName = uri.getHost().toLowerCase();
                if (!checkDomainROI.containsKey(siteName)) {
                    checkDomainROI.put(siteName, 0);
                }

                // 2. Download the web page.
                Document doc = Jsoup.connect(url).ignoreHttpErrors(true).ignoreContentType(true).get(); // <-- quick and easy?
                System.out.println("SCRAPING :: " + doc.title() + " FOR EMAILS");

                // 3. Scrape for emails and add them to emailSet().
                Pattern p = Pattern.compile("[a-zA-Z0-9_.+\\-]+@[a-zA-Z0-9\\-]+\\.[a-z]+");
                Matcher matcher = p.matcher(doc.text());
                // when matcher finds an email...
                while (matcher.find()) {

                    // ... and that email is unique,
                    if (!emailSet.contains(matcher.group())) {

                        // add the email
                        emailSet.add(matcher.group().toLowerCase());
                        lastUploadCount++;

                        // get the current email count, increment (count++)
                        // and insert the count into the ROI Tracker
                        int count = checkDomainROI.get(siteName);
                        count++;
                        checkDomainROI.put(siteName, count);

                    }
                }

                // 4. Scrape for links and add each of them to linksToVisit().
                Elements links = doc.select("a[href]");
                for (Element link : links) { // \t => tab stop

                    //System.out.println("Found :: " + String.format("%s\n\t%s", link.attr("title"), link.absUrl("href")));
                    String foundLink = link.absUrl("href");

                    if(!foundLink.isEmpty()){
                        // add the found links to linksToVisit
                        if ((!foundLink.contains("mailto:") && (!linksToVisit.contains(foundLink)))) {
                            linksToVisit.add(foundLink);
                        }
                    }
                    //System.out.println("FOUND LINK: " + foundLink);
                }
                // add the scraped link to linksVisited
                linksVisited.add(url);

//                System.out.println("------------------- KEY SET -------------------");
//
//                for(String link: checkDomainROI.keySet()){
//                    System.out.println(link + " ::: " + checkDomainROI.get(link));
//                }

                System.out.println("------DOMAINS------+------VISITED------+------TO VISIT------+------EMAILS------");
                System.out.println( "      " + checkDomainROI.size() + "                    "+linksVisited.size()
                        + "                   " + linksToVisit.size() + "                " + lastUploadCount); //emailSet.size());


            }// end of if(!bannedLinks.contains(url)


        } catch (Exception e) {
            e.printStackTrace();
            bannedLinks.add(url);
        }
    }


    public static void main(String[] args) {

        ExecutorService executor = Executors.newFixedThreadPool(1250); // <--- Factory method => When we call an object without using "new".

        linksToVisit.add("{START AT URL}");
        lastUploadCount = 0;

        while(linksToVisit.iterator().hasNext() && emailSet.size() <= 100){

            String visitLink = linksToVisit.iterator().next();
            if(!linksVisited.contains(visitLink)){
                executor.execute(new WorkerThread(visitLink));
            }
            // remove the scraped link from linksToVisit
            linksToVisit.remove(visitLink);

        }
        executor.shutdownNow();


//        try{
//            uploadToDB(emailSet);
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }

        for(String email: emailSet){
            System.out.println(email);
        }


        //getEmailsFromDB();

    }


    private static void uploadToDB(List<String> list) throws ClassNotFoundException{ //[INSERT INTO QUERY]

        System.out.println("*----------------------------------------*Uploading To DB*----------------------------------------*");
        String r = "";

        String url = "{URL}";
        String connectionURL = String.format("jdbc:sqlserver://%s;databaseName={DB NAME};user={USERNAME};password={PASSWORD}", url);


        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        try(Connection con = DriverManager.getConnection(connectionURL);
            Statement statement = con.createStatement() ) {

            for (String email: list) {
                String insertQuery = String.format("INSERT INTO Emails VALUES ('%s')", email);
                statement.execute(insertQuery);
            }

        }
        catch(SQLException e){
            e.printStackTrace();
            r = e.getMessage();
        }

        System.out.println(r);
    }


}
