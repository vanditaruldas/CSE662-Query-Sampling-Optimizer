package mimir.exec.mode

import mimir.Database
import mimir.optimizer.operator._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.exec._
import mimir.exec.result._
import mimir.models.Model
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.parser.MimirJSqlParser
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.schema.Column
import scala.io.Source
import java.util.Random
import java.util.HashMap
import java.util.HashSet
import scala.collection.mutable.StringBuilder

object CostOptimizer{
  
  val random = new Random(42)
  val seeds = (0 until 10).map { _ => random.nextLong }
  var limit = false
  val relevantTablesMap = Source.fromFile("test/UncertaintyList/UncertaintyList.txt").getLines.toArray.map{ line => 
    val w = line.split(" ")
    (w(0),w.tail)
  }.toMap
  
  var hybridTimings =Seq[(Double,Double,Double)]()
  var timings : HashMap[String, HashMap[String, HashMap[Integer, java.lang.Double]]] = null
  var uncertSet : HashSet[String] = null
  var naiveTiming : Double = 0.0
  var minVal = java.lang.Double.MAX_VALUE
  var approach = new StringBuilder("")
  var value : Double = 0.0
  var TBtoIL : Double = 1.0
  var ILtoTB : Double = 1.0
  var numUcSelect : Int = 0
  
  
  def calcHybrid(compileType : StringBuilder, index : Int) {
    if(index == hybridTimings.size) {
      println(s"$compileType $value")
      if(value<minVal) {
        minVal = value
        approach = new StringBuilder(compileType.toString) 
      }
    }
    else {
      if(hybridTimings(index)._1==(-1)) {
        compileType+='3'
        calcHybrid(compileType,index+1)
        compileType.deleteCharAt(compileType.size-1)
      } else if(value!=java.lang.Double.MAX_VALUE) {
        if(hybridTimings(index)._1!=java.lang.Double.MAX_VALUE) {
          var temp = hybridTimings(index)._1
          if(compileType.size>1 && compileType(compileType.size-1)!='0') {
            temp += ILtoTB
          }
          value+= temp
          compileType+='0'
          calcHybrid(compileType,index+1)
          value-= temp
          compileType.deleteCharAt(compileType.size-1)
        }
        var temp = hybridTimings(index)._3
        if(compileType.size>1 && compileType(compileType.size-1)!='2') {
          temp += TBtoIL
        }
        value+= temp
        compileType+='2'
        calcHybrid(compileType,index+1)
        value-= temp
        compileType.deleteCharAt(compileType.size-1)
      }
      
    }
  }
  
  def getCompileMode(db: Database,queryRaw: Operator, timingsF: HashMap[String, HashMap[String, HashMap[Integer, java.lang.Double]]], UCSet : HashSet[String]) : String =
  {  
    hybridTimings = Seq[(Double,Double,Double)]()
    var query = queryRaw
    timings = timingsF
    uncertSet = UCSet
    val (withProvenance, provenanceCols) = Provenance.compile(query)
    query = withProvenance
    val (compiled, nonDeterministicColumns) = compileTimings(query, db)
    println("\n"+hybridTimings)
    minVal = naiveTiming
    if(minVal!= java.lang.Double.MAX_VALUE) {
      println(s"N: $minVal")
    }
    approach = new StringBuilder("N")
    calcHybrid(new StringBuilder(""),0)
    println(s"CompileType: $approach \nMin Time: $minVal" )
    //Check timings
    approach.toString
  }
  
  def getNumUcSelect(condition : Expression) : Unit = {
    if(condition.children(0).isInstanceOf[Var] || condition.children(1).isInstanceOf[Var]) {
      if(!(condition.children(0).isInstanceOf[Var] && condition.children(1).isInstanceOf[Var])) {
        if(uncertSet.contains(condition.children(0).toString)) numUcSelect+=1
        if(uncertSet.contains(condition.children(1).toString)) numUcSelect+=1
      }
    }
    else{
      getNumUcSelect(condition.children(0))
      getNumUcSelect(condition.children(1))
    }      
  }

  def compileTimings(query: Operator, db: Database): (Operator, Set[String]) =
  {
    query match {
      case (Table(_,_,_,_) | EmptyTable(_)) =>
        (
          query.addColumn(
            WorldBits.columnName -> IntPrimitive(WorldBits.fullBitVector(seeds.size))
          ),
          Set[String]()
        )

      case Project(columns, oldChild) => {

        val (newChild, nonDeterministicInput) = compileTimings(oldChild, db)
        hybridTimings = hybridTimings :+ (-1.0,-1.0,-1.0)

        val (
          newColumns,
          nonDeterministicOutputs
          ):(Seq[ProjectArg], Seq[Set[String]]) = columns.map { col =>
          if(!CTables.isDeterministic(col.expression)){
            var clause1 = (Var(WorldBits.columnName).eq(IntPrimitive(1)),IntPrimitive(seeds(0)))
            var clause2 = (Var(WorldBits.columnName).eq(IntPrimitive(2)),IntPrimitive(seeds(1)))
            var clause3 = (Var(WorldBits.columnName).eq(IntPrimitive(4)),IntPrimitive(seeds(2)))
            var clause4 = (Var(WorldBits.columnName).eq(IntPrimitive(8)),IntPrimitive(seeds(3)))
            var clause5 = (Var(WorldBits.columnName).eq(IntPrimitive(16)),IntPrimitive(seeds(4)))
            var clause6 = (Var(WorldBits.columnName).eq(IntPrimitive(32)),IntPrimitive(seeds(5)))
            var clause7 = (Var(WorldBits.columnName).eq(IntPrimitive(64)),IntPrimitive(seeds(6)))
            var clause8 = (Var(WorldBits.columnName).eq(IntPrimitive(128)),IntPrimitive(seeds(7)))
            var clause9 = (Var(WorldBits.columnName).eq(IntPrimitive(256)),IntPrimitive(seeds(8)))
            var listExp:List[(Expression,Expression)] = List(clause1,clause2,clause3,clause4,clause5,clause6,clause7,clause8,clause9)
            var nullMap:Map[String,Expression] = Map()
            (ProjectArg(col.name,
              CTAnalyzer.compileSample(
                Eval.inline(col.expression, nullMap),
                ExpressionUtils.makeCaseExpression(listExp,IntPrimitive(seeds(9))),
                db.models.get(_)
              )) ,
              Set(col.name)
            )
          } else {
            (col, Set[String]())
          }
        }.unzip

        val replacementProjection =
          Project(
            newColumns ++ Seq(ProjectArg(WorldBits.columnName, Var(WorldBits.columnName))),
            newChild
          )

        (replacementProjection, nonDeterministicOutputs.flatten.toSet)
      }

      case Select(condition, oldChild) => {
        numUcSelect = 0
        //println("\nselect columns: "+query.expressions)
        getNumUcSelect(condition)
        var numUcCols = numUcSelect
        if(numUcCols>2) numUcCols = 2
        var tb = java.lang.Double.MAX_VALUE
        var il = java.lang.Double.MAX_VALUE
        var naive = java.lang.Double.MAX_VALUE
        if(timings.containsKey("selection")) {
          val timingsSel = timings.get("selection")
          if(timingsSel.containsKey("TB")) {
            tb=timingsSel.get("TB").get(numUcCols)
          }
          if(timingsSel.containsKey("Naive")) {
            naiveTiming+=timingsSel.get("Naive").get(numUcCols)
          }
          if(timingsSel.containsKey("IL")) {
            il=timingsSel.get("IL").get(numUcCols)
          }
        } 
        
        
        //println(condition)
        val (newChild, nonDeterministicInput) = compileTimings(oldChild, db)
        hybridTimings = hybridTimings :+ (tb,naive,il)
        //println(nonDeterministicInput)
//          ( Select(ExpressionUtils.makeAnd(condition,ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(Var(WorldBits.columnName).eq(IntPrimitive(1)),
//            Var(WorldBits.columnName).eq(IntPrimitive(2))),Var(WorldBits.columnName).eq(IntPrimitive(2)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(4))),Var(WorldBits.columnName).eq(IntPrimitive(8)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(16))),Var(WorldBits.columnName).eq(IntPrimitive(32)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(64))),Var(WorldBits.columnName).eq(IntPrimitive(128)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(256))),Var(WorldBits.columnName).eq(IntPrimitive(512)))), newChild), nonDeterministicInput )

        (Select(condition,newChild),nonDeterministicInput)
      }

      case Join(lhsOldChild, rhsOldChild) => {
        
        var numUcCols = 0
        var tb = java.lang.Double.MAX_VALUE
        var il = java.lang.Double.MAX_VALUE
        var naive = java.lang.Double.MAX_VALUE
        if(timings.containsKey("join")) {
          val timingsSel = timings.get("join")
          if(timingsSel.containsKey("TB")) {
            tb=timingsSel.get("TB").get(numUcCols)
          }
          if(timingsSel.containsKey("Naive")) {
            naiveTiming+=timingsSel.get("Naive").get(numUcCols)
          }
          if(timingsSel.containsKey("IL")) {
            il=timingsSel.get("IL").get(numUcCols)
          }
        } 
        
        //println("lhs: "+lhsOldChild+"\n"+rhsOldChild)
        val (lhsNewChild, lhsNonDeterministicInput) = compileTimings(lhsOldChild, db)
        //println(lhsNonDeterministicInput)
        val (rhsNewChild, rhsNonDeterministicInput) = compileTimings(rhsOldChild, db)
        //println(rhsNonDeterministicInput)
        // To safely join the two together, we need to rename the world-bit columns

        hybridTimings = hybridTimings :+ (tb,naive,il)
        val rewrittenJoin =
                  OperatorUtils.joinMergingColumns(
                    Seq( (WorldBits.columnName,
                      (lhs:Expression, rhs:Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
                    ),
                    lhsNewChild, rhsNewChild
                  )

        val completedJoin =
          Select(
            Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
            rewrittenJoin
          )

        (completedJoin, lhsNonDeterministicInput ++ rhsNonDeterministicInput)
      }

      case Union(lhsOldChild, rhsOldChild) => {
        val (lhsNewChild, lhsNonDeterministicInput) = compileTimings(lhsOldChild, db)
        val (rhsNewChild, rhsNonDeterministicInput) = compileTimings(rhsOldChild, db)
        hybridTimings = hybridTimings :+ (-1.0,-1.0,-1.0)
        val nonDeterministicOutput =
          lhsNonDeterministicInput ++ rhsNonDeterministicInput
        (
          Union(
            lhsNewChild,
            rhsNewChild
          ),
          nonDeterministicOutput
        )
      }

      case Aggregate(gbColumns, aggColumns, oldChild) => {
        var numUcCols = 0 //TODO
        gbColumns.map {
          col => {
            if(uncertSet.contains(col.toString)) numUcCols+=1
          }
        }
        if(numUcCols>2) numUcCols = 2
        var tb = java.lang.Double.MAX_VALUE
        var il = java.lang.Double.MAX_VALUE
        var naive = java.lang.Double.MAX_VALUE
        if(timings.containsKey("groupby")) {
          val timingsSel = timings.get("groupby")
          if(timingsSel.containsKey("TB")) {
            tb=timingsSel.get("TB").get(numUcCols)
          }
          if(timingsSel.containsKey("Naive")) {
            naiveTiming+=timingsSel.get("Naive").get(numUcCols)
          }
          if(timingsSel.containsKey("IL")) {
            il=timingsSel.get("IL").get(numUcCols)
          }
        } 
        
        //var numUcCols = 0
        //println(relevantTablesMap)
        /*for (col <- gbColumns) {
          val splitcols = col.name.split("_")
          if(relevantTablesMap.contains(splitcols(0))) {
            if(relevantTablesMap.apply(splitcols(0)).contains(splitcols(3))) {
              numUcCols+=1
            }
          }
        }
        println(s"num: $numUcCols")
        for (col <- gbColumns) {
          println(col.name) 
        }
        */
        val (newChild, nonDeterministicInput) = compileTimings(oldChild, db)
        hybridTimings = hybridTimings :+ (tb,naive,il)
        (
          Aggregate(gbColumns++Seq(Var(WorldBits.columnName)), aggColumns,
            newChild),nonDeterministicInput
        )
      }


      case Limit(offset,count,oldChild) =>{
        limit = true
        val (newChild, nonDeterministicInput) = compileTimings(oldChild, db)
        hybridTimings = hybridTimings :+ (-1.0,-1.0,-1.0)
        limit = false
        var projectArgs = newChild.columnNames.map{ cols =>
          ProjectArg(cols,Var(cols))
        }
        val sampleShards = (0 until 10).map { i =>
          Project(projectArgs,Limit(offset,count,Select(
          Comparison(Cmp.Eq, Var(WorldBits.columnName), IntPrimitive(Math.pow(2,i).toLong))
          ,Project(projectArgs, newChild))))
        }


        (
          OperatorUtils.makeUnion(sampleShards),nonDeterministicInput
        )

      }


      case Sort(sortCols,oldChild)=>{
        var numUcCols = 0 //TODO
        sortCols.map {
          col => {
            if(uncertSet.contains(col.expression.toString)) numUcCols+=1
          }
        }
        if(numUcCols>2) numUcCols = 2
        var tb = java.lang.Double.MAX_VALUE
        var il = java.lang.Double.MAX_VALUE
        var naive = java.lang.Double.MAX_VALUE
        if(timings.containsKey("orderby")) {
          val timingsSel = timings.get("orderby")
          if(timingsSel.containsKey("TB")) {
            tb=timingsSel.get("TB").get(numUcCols)
          }
          if(timingsSel.containsKey("Naive")) {
            naiveTiming+=timingsSel.get("Naive").get(numUcCols)
          }
          if(timingsSel.containsKey("IL")) {
            il=timingsSel.get("IL").get(numUcCols)
          }
        } 
        
        //println(query)
        val (newChild, nonDeterministicInput) = compileTimings(oldChild, db)
        hybridTimings = hybridTimings :+ (tb,naive,il)
        //println(nonDeterministicInput)
        if (limit){
          (
            Sort(sortCols,newChild),nonDeterministicInput
          )
        }

        else{


          var SortCols:Seq[SortColumn]=Nil
          var newCol:SortColumn = new SortColumn(Var(WorldBits.columnName),true)
          SortCols :+= newCol
          sortCols.map{col=>
            SortCols :+= col
          }
          (
            Sort(SortCols,newChild),nonDeterministicInput
          )

        }


      }


      // We don't handle materialized tuple bundles (at the moment)
      // so give up and drop the view.
      case View(_, query, _) =>  compileTimings(query, db)




      case ( LeftOuterJoin(_,_,_) | Annotate(_, _) | ProvenanceOf(_) | Recover(_, _) ) =>
        throw new RAException("Interleave mode presently doesn't support LeftOuterJoin")
    }
  }
  
  
}