package mimir.timing.vldb2017

import java.io._

import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.concurrent._

import scala.concurrent.duration._
import mimir.algebra._
import mimir.util._
import mimir.ctables.InlineVGTerms
import mimir.optimizer.operator.InlineProjections
import mimir.test.{PDBench, SQLTestSpecification, TestTimer}
import mimir.models._
import mimir.exec.uncertainty._
import mimir.parser.MimirJSqlParser
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, Select, SelectBody}

object ImputeTiming
  extends VLDB2017TimingTest("tpch10_tpch_UC1", Map("reset" -> "NO", "inline" -> "YES"))//, "initial_db" -> "test/tpch-impute-1g.db"))
  with BeforeAll
{

  sequential

  val fullReset = true
  val runBestGuessQueries = false
  val runTupleBundleQueries = true
  val runSamplerQueries = false
  val useMaterialized = false
  val useFastPathCache = false

  val timeout = 15.minute

  def beforeAll =
  {
    if(fullReset){
      println("DELETING ALL MIMIR METADATA")
      update("DELETE FROM MIMIR_MODEL_OWNERS")
      update("DELETE FROM MIMIR_MODELS")
      update("DELETE FROM MIMIR_VIEWS")
    }
  }

  val relevantTables = Seq(
    ("LINEITEM", Seq("linestatus","discount","orderkey")),
    //("LINEITEM", Seq("shipdate")),
    ("ORDERS", Seq("custkey"))
    //("ORDERS", Seq("orderdate"))
  )

  val relevantIndexes = Seq(

    ("LINEITEM", Seq(
      Seq("linestatus","discount","orderkey")
    )),
      ("orders", Seq(
        Seq("custkey","orderkey")
      ))
  )
  if(false){ "Skipping TPCH Inpute Test" >> ok } else {
    "TPCH Impute" should {

      sequential
      Fragments.foreach(1 to 1){ i =>

        val TPCHQueries = 
          Seq(        
            /*
            s"""
            select p.type from lineitem_run_$i as l,part as p where l.partkey = p.partkey and p.mfgr = 'Manufacturer#1' 
            and p.brand = 'Brand#13'and  p.size = 50 and l.tax = 0.02;
            """
            ,*/
            s"""
            select l.partkey,l.quantity from lineitem_run_$i as l,orders_run_$i as o 
            where l.orderkey = o.orderkey and o.orderpriority = '1-URGENT' and o.totalprice > 430000 and o.orderstatus = 'F';
            """
            /*
            ,
            
            s"""
            SELECT l.orderkey
 FROM lineitem_run_$i as l, orders_run_$i as o
 WHERE o.orderdate = l.shipdate and l.quantity = 50 and l.returnflag = 'R' and l.discount = 0.1 and l.extendedprice>95000;
            """
            */
            /*,
            s"""
            SELECT orderkey,discount from  lineitem_run_$i where  linestatus = 'F' and discount > 0.07
            and tax = 0.03 and extendedprice > 84000 and quantity > 49 and returnflag = 'R' ;
               """
               ,
               s"""
               SELECT linenumber from lineitem_run_$i  where quantity > 49 and returnflag = 'R'  and shipmode = 'FOB' and extendedprice>84000 order by linenumber;
               """,
               s"""
               select discount from lineitem_run_$i  where quantity > 49 and returnflag = 'R'  and shipmode = 'FOB' and extendedprice>84000 order by discount;
               """
               ,
               s"""
               select discount from lineitem_run_$i  where quantity > 49 and returnflag = 'R'  and shipmode = 'FOB' and extendedprice>84000 order by discount, linestatus;
               """
               */
            // ,
            // s"""
            //   -- TPC-H Query 3
            //   SELECT o.orderkey, 
            //          o.orderdate,
            //          o.shippriority,
            //          SUM(extendedprice * (1 - discount)) AS query3
            //   FROM   customer_run_$i c, orders_run_$i o, lineitem_run_$i l
            //   WHERE  c.mktsegment = 'BUILDING'
            //     AND  o.custkey = c.custkey
            //     AND  l.orderkey = o.orderkey
            //     AND  o.orderdate < DATE('1995-03-15')
            //     AND  l.shipdate > DATE('1995-03-15')
            //   GROUP BY o.orderkey, o.orderdate, o.shippriority;
            // """
            // ,
            // s"""
            //   -- TPC-H Query 5 - NoAgg
            //   SELECT n.name, l.extendedprice * (1 - l.discount) AS revenue 
            //    FROM   region r, nation_run_$i n, supplier_run_$i s, customer_run_$i c, orders_run_$i o, lineitem_run_$i l 
            //   WHERE  r.name = 'ASIA'
            //     AND  n.regionkey = r.regionkey 
            //     AND  c.custkey = o.custkey
            //     AND  o.orderdate >= DATE('1994-01-01')
            //     AND  o.orderdate <  DATE('1995-01-01')
            //     AND  l.orderkey = o.orderkey 
            //     AND  l.suppkey = s.suppkey
            //     AND  c.nationkey = s.nationkey 
            //     AND  s.nationkey = n.nationkey 
            // """
            // ,
            // s"""
            //   -- TPC-H Query 5
            //   SELECT n.name, SUM(l.extendedprice * (1 - l.discount)) AS revenue 
            //    FROM   region r, nation_run_$i n, supplier_run_$i s, customer_run_$i c, orders_run_$i o, lineitem_run_$i l 
            //   WHERE  r.name = 'ASIA'
            //     AND  n.regionkey = r.regionkey 
            //     AND  c.custkey = o.custkey
            //     AND  o.orderdate >= DATE('1994-01-01')
            //     AND  o.orderdate <  DATE('1995-01-01')
            //     AND  l.orderkey = o.orderkey 
            //     AND  l.suppkey = s.suppkey
            //     AND  c.nationkey = s.nationkey 
            //     AND  s.nationkey = n.nationkey 
            //   GROUP BY n.name
            // """
            // ,
            // s"""
            //   -- TPC-H Query 9
            //   SELECT nation, o_year, SUM(amount) AS sum_profit 
            //   FROM (
            //     SELECT n.name AS nation, 
            //            EXTRACT(year from o.orderdate) AS o_year,
            //            ((l.extendedprice * (1 - l.discount)) - (ps.supplycost * l.quantity))
            //               AS amount
            //     FROM  part p, 
            //           partsupp_run_$i ps, 
            //           supplier_run_$i s, 
            //           lineitem_run_$i l, 
            //           orders_run_$i o, 
            //           nation_run_$i n
            //     WHERE  (p.name LIKE '%green%')
            //       AND  ps.partkey = l.partkey
            //       AND  ps.suppkey = l.suppkey 
            //       AND  p.partkey = l.partkey
            //       AND  s.suppkey = l.suppkey
            //       AND  o.orderkey = l.orderkey 
            //       AND  s.nationkey = n.nationkey 
            //     ) AS profit 
            //   GROUP BY nation, o_year;
            // """
          )


        sequential

        // CREATE LENSES
        Fragments.foreach(
          relevantTables.toSeq
        ){ createMissingValueLens(_, s"_RUN_$i") }

        // // INDEXES
        if(useMaterialized){
          Fragments.foreach( relevantIndexes ) {
            case (baseTable, indices) => 
              val viewTable = s"${baseTable}_RUN_$i"
              Fragments.foreach(indices){ index =>
                s"Create Index $viewTable(${index.mkString(",")})" >> {
                  val indexName = viewTable+"_"+index.mkString("_")
                  println(s"CREATE INDEX $indexName")
                  db.backend.update(s"CREATE INDEX IF NOT EXISTS $indexName ON $viewTable(${index.mkString(",")})")
                  ok
                }
              }
          }
        } else { "No Need To Create Indexes" >> ok }

        // QUERIES
        if(runBestGuessQueries){
          Fragments.foreach( TPCHQueries.zipWithIndex )
          {
            queryLens(_)
          }
        } else { "Skipping Best Guess Tests" >> ok }

        if(runTupleBundleQueries){
          Fragments.foreach(TPCHQueries.zipWithIndex)
          {
            sampleFromLens(_)
          }
        }else { "Skipping Tuple Bundle Tests" >> ok }

        if(runSamplerQueries){
          Fragments.foreach(TPCHQueries.zipWithIndex)
          {
            expectedFromLens(_)
          }
        } else { "Skipping Sampler Tests" >> ok }


      }

    }
  }
}