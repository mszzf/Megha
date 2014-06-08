import java.util.Scanner;
import com.mongodb.*;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.*;

public class GenericParser {

    public static void main(String[] args) {

        String sparqlQuery="";

        String sparqlQuery1 = "select ?a where { <http://purl.uniprot.org/citations/7934828> <http://purl.uniprot.org/core/author> ?a . }";
        String sparqlQuery2 = "select ?p ?o where { <http://purl.uniprot.org/uniprot/Q6GZX4> ?p ?o . }";
        String sparqlQuery3 = "select ?x ?y where { ?x <http://purl.uniprot.org/core/name> Virology . ?x <http://purl.uniprot.org/core/volume> ?y . }";
        String sparqlQuery4 = "select ?x ?z where { ?x <http://purl.uniprot.org/core/name> ?y . ?x <http://purl.uniprot.org/core/volume> ?z . ?x <http://purl.uniprot.org/core/pages> 176-186 . }";
        String sparqlQuery5 = "select ?x ?y ?z where { ?x <http://purl.uniprot.org/core/name> Science . ?x <http://purl.uniprot.org/core/author> ?y . ?z <http://purl.uniprot.org/core/citation> ?x . }";
        String sparqlQuery6 = "select ?x ?y where { ?x ?y Israni S. . <http://purl.uniprot.org/citations/15372022> ?y Gomez M. . }";
        String sparqlQuery7 = "select ?a ?b where { ?x ?y <http://purl.uniprot.org/citations/15165820> . ?a ?b ?y . }";
        String sparqlQuery8 = "select ?x ?z ?a where { ?x <http://purl.uniprot.org/core/reviewed> ?y . ?x <http://purl.uniprot.org/core/created> ?b . ?x <http://purl.uniprot.org/core/mnemonic> 003L_IIV3 . ?x <http://purl.uniprot.org/core/citation> ?z . ?z <http://purl.uniprot.org/core/author> ?a . }";



        System.out.println("Please enter the query you wish to execute from Query 1 to 8?");


        Scanner scanner=new Scanner(System.in);

        int queryNumber=scanner.nextInt();
       switch (queryNumber) {

            case 1:  sparqlQuery= sparqlQuery1;
                break;
            case 2:  sparqlQuery= sparqlQuery2;
                break;
            case 3:  sparqlQuery= sparqlQuery3;
                break;
            case 4:  sparqlQuery= sparqlQuery4;
                break;
            case 5:  sparqlQuery= sparqlQuery5;
                break;
            case 6:  sparqlQuery= sparqlQuery6;
                break;
            case 7:  sparqlQuery= sparqlQuery7;
                break;
            case 8:  sparqlQuery= sparqlQuery8;
                break;


        }


        String[] queryParts = sparqlQuery.split(" where \\{ ");

        ComplexQuery complexQuery = new ComplexQuery();
        complexQuery.selectString = queryParts[0];
        String[] selectStringParts = complexQuery.selectString.split(" ");

        for (int i = 1; i < selectStringParts.length; i++) {
            complexQuery.selectVariables.add(selectStringParts[i]);
        }


        String[] whereParts = queryParts[1].split(" . ");

        String[] queryFields;
        for (int i = 0; i < whereParts.length - 1; i++) {
            complexQuery.subQueryStrings.add(whereParts[i]);
            queryFields = whereParts[i].split(" ",3);

            Tuple tuple = new Tuple(queryFields[0], queryFields[1], queryFields[2]);

            complexQuery.addSubQuery(tuple);
        }


        complexQuery.executeQuery(false);

        complexQuery.printResults();
    }
}


class Tuple {
    String subject;
    String predicate;
    String object;
    List<String> variables = new ArrayList<String>();

    Tuple(String subject, String predicate, String object) {

        this.subject = subject;
        this.predicate = predicate;
        this.object = object;


        if (isVariable(subject)) {

            variables.add(subject);
        }
        if (isVariable(predicate)) {

            variables.add(predicate);
        }
        if (isVariable(object)) {

            variables.add(object);
        }

    }

    boolean isVariable(String field) {

        return field.charAt(0) == '?';

    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).append(subject).append(object).append(predicate).toHashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof Tuple))
            return false;

        Tuple rhs = (Tuple) obj;
        return new EqualsBuilder().append(subject, rhs.subject).append(predicate, rhs.predicate).append(object, rhs.object).isEquals();
    }
}


class ResultTuple {
    Tuple result;
    Tuple query;

    ResultTuple(String subject, String predicate, String object, Tuple query) {
        result = new Tuple(subject, predicate, object);
        this.query = query;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).append(result).append(query).toHashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof ResultTuple))
            return false;

        ResultTuple rhs = (ResultTuple) obj;
        return new EqualsBuilder().append(result, rhs.result).append(query, rhs.query).isEquals();
    }
}

class Table {
    List<Row> rows = new ArrayList<Row>();
    List<String> columnNames = new ArrayList<String>();
}


class Row {
    Map<String, Column> columns = new HashMap<String, Column>();
}

class Column {
    String name;
    String data;

    Column(String name, String data) {
        this.name = name;
        this.data = data;
    }
}


class ComplexQuery {

    MongoClient mongoClient;
    DB db;
    DBCollection coll;
    Table table = new Table();

    List<Tuple> subQueries = new ArrayList<Tuple>();

    ComplexQuery() {
        try {
            mongoClient = new MongoClient("localhost", 27017);
        } catch (Exception ignore) {

        }
        db = mongoClient.getDB("test");
        System.out.println("Connect to database successfully");
        coll = db.getCollection("mycol999");

    }

    void addSubQuery(Tuple query) {
        subQueries.add(query);
        if (query.isVariable(query.subject)) {
            if (!table.columnNames.contains(query.subject)) {
                table.columnNames.add(query.subject);
            }
        }
        if (query.isVariable(query.predicate)) {
            if (!table.columnNames.contains(query.predicate)) {
                table.columnNames.add(query.predicate);
            }
        }
        if (query.isVariable(query.object)) {
            if (!table.columnNames.contains(query.object)) {
                table.columnNames.add(query.object);
            }
        }
    }

    String selectString;
    List<String> subQueryStrings = new ArrayList<String>();
    List<String> selectVariables = new ArrayList<String>();

    Map<Tuple, List<ResultTuple>> executeQuery(boolean distinct) {
        Map<Tuple, List<ResultTuple>> complexQueryResult = new HashMap<Tuple, List<ResultTuple>>();
        Collections.sort(subQueries, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                Tuple tuple1 = (Tuple) o1;
                Tuple tuple2 = (Tuple) o2;
                int returnVal = 0;
                if (tuple1.variables.size() < tuple2.variables.size()) {       //o1.var<o2.var then o1<o2 so o1 goes first
                    returnVal = -1;
                } else if (tuple1.variables.size() > tuple2.variables.size()) {
                    returnVal = 1;
                }
                return returnVal;
            }
        });

        for (Tuple subQuery : subQueries) {
            ArrayList<ResultTuple> result = new ArrayList<ResultTuple>();

            if (table.rows.size() == 0) {
                BasicDBObject dbObject = new BasicDBObject();

                if (!subQuery.isVariable(subQuery.subject)) {
                    dbObject.put("Subject", "\"" + subQuery.subject + "\"");
                }

                if (!subQuery.isVariable(subQuery.predicate)) {
                    dbObject.put("Predicate", "\"" + subQuery.predicate + "\"");
                }
                if (!subQuery.isVariable(subQuery.object)) {
                    dbObject.put("Object", "\"" + subQuery.object + "\"");
                }
                DBCursor cursor_current = coll.find(dbObject);
                while (cursor_current.hasNext()) {
                    DBObject dbobject1 = cursor_current.next();

                    ResultTuple resultTuple = new ResultTuple((String) dbobject1.get("Subject"), (String) dbobject1.get("Predicate"), (String) dbobject1.get("Object"), subQuery);
                    if (!distinct || !result.contains(resultTuple)) {
                        result.add(resultTuple);
                        Row row = new Row();
                        for (String columnName : table.columnNames) {
                            String columnValue = null;
                            if (columnName.equals(subQuery.subject)) {
                                columnValue = resultTuple.result.subject;
                            } else if (columnName.equals(subQuery.predicate)) {
                                columnValue = resultTuple.result.predicate;
                            } else if (columnName.equals(subQuery.object)) {
                                columnValue = resultTuple.result.object;
                            }
                            row.columns.put(columnName, new Column(columnName, columnValue));
                        }
                        table.rows.add(row);
                    }
                }
            } else {
                List<Row> rows = new ArrayList<Row>();
                rows.addAll(table.rows);
                table.rows.clear();
                List<ResultTuple> previousRowResult=new ArrayList<ResultTuple>();
                for (Row row : rows) {
                    BasicDBObject dbObject = new BasicDBObject();
                    BasicDBObject dbObjectPrevious = new BasicDBObject();
                    int previousRowIndex = rows.indexOf(row) - 1;
                    if (previousRowIndex > -1) {
                        Row previousRow = rows.get(previousRowIndex);
                        if (!subQuery.isVariable(subQuery.subject)) {
                            dbObjectPrevious.put("Subject", "\"" + subQuery.subject + "\"");
                        } else if (previousRow.columns.get(subQuery.subject).data != null) {
                            dbObjectPrevious.put("Subject", previousRow.columns.get(subQuery.subject).data);
                        }

                        if (!subQuery.isVariable(subQuery.predicate)) {
                            dbObjectPrevious.put("Predicate", "\"" + subQuery.predicate + "\"");
                        } else if (previousRow.columns.get(subQuery.predicate).data != null) {
                            dbObjectPrevious.put("Predicate", previousRow.columns.get(subQuery.predicate).data);
                        }

                        if (!subQuery.isVariable(subQuery.object)) {
                            dbObjectPrevious.put("Object", "\"" + subQuery.object + "\"");
                        } else if (previousRow.columns.get(subQuery.object).data != null) {
                            dbObjectPrevious.put("Object", previousRow.columns.get(subQuery.object).data);
                        }

                    }


                    if (!subQuery.isVariable(subQuery.subject)) {
                        dbObject.put("Subject", "\"" + subQuery.subject + "\"");
                    } else if (row.columns.get(subQuery.subject).data != null) {
                        dbObject.put("Subject", row.columns.get(subQuery.subject).data);
                    }

                    if (!subQuery.isVariable(subQuery.predicate)) {
                        dbObject.put("Predicate", "\"" + subQuery.predicate + "\"");
                    } else if (row.columns.get(subQuery.predicate).data != null) {
                        dbObject.put("Predicate", row.columns.get(subQuery.predicate).data);
                    }

                    if (!subQuery.isVariable(subQuery.object)) {
                        dbObject.put("Object", "\"" + subQuery.object + "\"");
                    } else if (row.columns.get(subQuery.object).data != null) {
                        dbObject.put("Object", row.columns.get(subQuery.object).data);
                    }

                    List<ResultTuple> currentRowResult=new ArrayList<ResultTuple>();

                    if (!dbObject.equals(dbObjectPrevious)) {
                        DBCursor cursor_current = coll.find(dbObject);
                        while (cursor_current.hasNext()) {
                            DBObject dbobject1 = cursor_current.next();
                            ResultTuple resultTuple = new ResultTuple((String) dbobject1.get("Subject"), (String) dbobject1.get("Predicate"), (String) dbobject1.get("Object"), subQuery);
                            if (!distinct || !currentRowResult.contains(resultTuple)) {
                                currentRowResult.add(resultTuple);
                            }
                        }
                    }
                    else{
                        currentRowResult.addAll(previousRowResult);
                    }

                    for(ResultTuple resultTuple: currentRowResult){
                        Row rowTemp = new Row();
                        for (String columnName : table.columnNames) {
                            String columnValue = row.columns.get(columnName).data;
                            if (columnName.equals(subQuery.subject)) {
                                columnValue = resultTuple.result.subject;
                            } else if (columnName.equals(subQuery.object)) {
                                columnValue = resultTuple.result.object;
                            } else if (columnName.equals(subQuery.predicate)) {
                                columnValue = resultTuple.result.predicate;
                            }
                            rowTemp.columns.put(columnName, new Column(columnName, columnValue));
                        }
                        table.rows.add(rowTemp);
                    }
                    result.addAll(currentRowResult);
                    previousRowResult.clear();
                    previousRowResult.addAll(currentRowResult);

                }
            }
            complexQueryResult.put(subQuery, result);
        }
        return complexQueryResult;
    }

    void printResults() {
        for (Row row : table.rows) {
            {
                for (String columnName : selectVariables) {
                    System.out.print(row.columns.get(columnName).data + "\t");
                }
                System.out.println();
            }
        }
        System.out.println("The total number of records is " + table.rows.size());
    }
}