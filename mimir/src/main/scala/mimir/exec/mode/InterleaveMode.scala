package mimir.exec.mode

import java.io.ByteArrayInputStream

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

/**
  * TupleBundles ( http://dl.acm.org/citation.cfm?id=1376686 ) are a tactic for
  * computing over probabilistic data.  Loosely put, the approach is to compile
  * the query to evaluate simultaneously in N possible worlds.  The results can
  * then be aggregated to produce an assortment of statistics, etc...
  *
  * This class actually wraps three different compilation strategies inspired
  * by tuple bundles, each handling parallelization in a slightly different way
  *
  * * **Long**:  Not technically "TupleBundles".  This approach simply unions
  * *            together a set of results, one per possible world sampled.
  * * **Flat**:  Creates a wide result, splitting each non-deterministic column
  *              into a set of columns, one per sample.
  * * **Array**: Like flat, but uses native array types to avoid overpopulating
  *              the result schema.
  *
  * At present, only 'Flat' is fully implemented, although a 'Long'-like approach
  * can be achieved by using convertFlatToLong.
  */

class InterleaveMode(seeds: Seq[Long] = (0l until 10l).toSeq)
  extends CompileMode[SampleResultIterator]
    with LazyLogging
{
  var limit = false
  type MetadataT =
    (
      Set[String],   // Nondeterministic column set
        Seq[String]    // Provenance columns
      )

  def rewrite(db: Database, queryRaw: Operator): (Operator, Seq[String], MetadataT) =
  {


    var query = queryRaw

    val (withProvenance, provenanceCols) = Provenance.compile(query)
    query = withProvenance

    val (compiled, nonDeterministicColumns) = compileInterleaved(query, db)
    query = compiled
    query = db.views.resolve(query)
    println(query)
    (
      query,
      //TO-DO check if this is right
      query.columnNames,
      (nonDeterministicColumns, provenanceCols)
    )
  }


  def getWorldBitQuery(db: Database): Operator =
  {
    if(!db.tableExists("WORLDBits")){
      db.backend.update(s"""
        CREATE TABLE WORLDBits(
          MIMIR_WORLD_BITS int,
          PRIMARY KEY (MIMIR_WORLD_BITS)
        )
      """)
      val bits = List(1, 2, 4, 8, 16,32,64,128,256,512)
      db.backend.fastUpdateBatch(s"""
        INSERT INTO WORLDBits (MIMIR_WORLD_BITS) VALUES (?);
      """,
        bits.map { bit =>
          Seq(IntPrimitive(bit))
        }
      )
    }
    var queryString = "select * from WORLDBits"
    var parser = new MimirJSqlParser(new ByteArrayInputStream(queryString.getBytes));
    val stmt: Statement = parser.Statement();
    (db.sql.convert(stmt.asInstanceOf[net.sf.jsqlparser.statement.select.Select]))
  }

  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): SampleResultIterator =
  {
    new SampleResultIterator(
      results,
      db.typechecker.schemaOf(query),
      meta._1,
      seeds.size
    )
  }

  def doesExpressionNeedSplit(expression: Expression, nonDeterministicInputs: Set[String]): Boolean =
  {
    val allInputs = ExpressionUtils.getColumns(expression)
    val expressionHasANonDeterministicInput =
      allInputs.exists { nonDeterministicInputs(_) }
    val expressionIsNonDeterministic =
      !CTables.isDeterministic(expression)

    return expressionHasANonDeterministicInput || expressionIsNonDeterministic
  }

  def splitExpressionsByWorlds(expressions: Seq[Expression], nonDeterministicInputs: Set[String], models: (String => Model)): Seq[Seq[Expression]] =
  {
    val outputColumns =
      seeds.zipWithIndex.map { case (seed, i) =>
        val inputInstancesInThisSample =
          nonDeterministicInputs.
            map { x => (x -> Var(TupleBundle.colNameInSample(x, i)) ) }.
            toMap
        expressions.map { expression =>
          CTAnalyzer.compileSample(
            Eval.inline(expression, inputInstancesInThisSample),
            IntPrimitive(seed),
            models
          )
        }
      }

    outputColumns
  }

  def splitExpressionByWorlds(expression: Expression, nonDeterministicInputs: Set[String], models: (String => Model)): Seq[Expression] =
  {
    splitExpressionsByWorlds(Seq(expression), nonDeterministicInputs, models).map(_(0))
  }

  def convertFlatToLong(compiledQuery: Operator, baseSchema: Seq[String], nonDeterministicInput: Set[String]): Operator =
  {
    val sampleShards =
      (0 until seeds.size).map { i =>
        val mergedSamples =
          baseSchema.map { col =>
            ProjectArg(col,
              if(nonDeterministicInput(col)){ Var(TupleBundle.colNameInSample(col, i)) }
              else { Var(col) }
            )
          } ++ Seq(
            ProjectArg(WorldBits.columnName, IntPrimitive(1 << i))
          )

        val filterWorldPredicate =
          Comparison(Cmp.Eq,
            Arithmetic(Arith.BitAnd,
              Var(WorldBits.columnName),
              IntPrimitive(1 << i)
            ),
            IntPrimitive(1 << i)
          )

        InlineProjections(
          ProjectRedundantColumns(
            Project(
              mergedSamples,
              PushdownSelections(
                Select(filterWorldPredicate, compiledQuery)
              )
            )
          )
        )
      }

    //    logger.trace(s"Converting FlatToLong: \n$compiledQuery\n ---> TO:\n${sampleShards(0)}")

    OperatorUtils.makeUnion(sampleShards)
  }

  def compileInterleaved(query: Operator, db: Database): (Operator, Set[String]) =
  {

    // Check for a shortcut opportunity... if the expression is deterministic, we're done!
    if(CTables.isDeterministic(query)){
      var worldQuery = getWorldBitQuery(db)
      var joinQuery = Join(query,worldQuery)
      var projectArgs = joinQuery.columnNames.map{ cols =>
        ProjectArg(cols,Var(cols))
      }
      var projQuery = Project(projectArgs,joinQuery)
      return (
        projQuery,
        Set[String]()
      )
    }
    query match {
      case (Table(_,_,_,_) | EmptyTable(_)) =>
        (
          query.addColumn(
            WorldBits.columnName -> IntPrimitive(WorldBits.fullBitVector(seeds.size))
          ),
          Set[String]()
        )

      case Project(columns, oldChild) => {

        val (newChild, nonDeterministicInput) = compileInterleaved(oldChild, db)

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

        val (newChild, nonDeterministicInput) = compileInterleaved(oldChild, db)
//          ( Select(ExpressionUtils.makeAnd(condition,ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(ExpressionUtils.makeOr(Var(WorldBits.columnName).eq(IntPrimitive(1)),
//            Var(WorldBits.columnName).eq(IntPrimitive(2))),Var(WorldBits.columnName).eq(IntPrimitive(2)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(4))),Var(WorldBits.columnName).eq(IntPrimitive(8)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(16))),Var(WorldBits.columnName).eq(IntPrimitive(32)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(64))),Var(WorldBits.columnName).eq(IntPrimitive(128)))
//            ,Var(WorldBits.columnName).eq(IntPrimitive(256))),Var(WorldBits.columnName).eq(IntPrimitive(512)))), newChild), nonDeterministicInput )

        (Select(condition,newChild),nonDeterministicInput)
      }

      case Join(lhsOldChild, rhsOldChild) => {

        val (lhsNewChild, lhsNonDeterministicInput) = compileInterleaved(lhsOldChild, db)
        val (rhsNewChild, rhsNonDeterministicInput) = compileInterleaved(rhsOldChild, db)

        // To safely join the two together, we need to rename the world-bit columns


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

        val (lhsNewChild, lhsNonDeterministicInput) = compileInterleaved(lhsOldChild, db)
        val (rhsNewChild, rhsNonDeterministicInput) = compileInterleaved(rhsOldChild, db)

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
        val (newChild, nonDeterministicInput) = compileInterleaved(oldChild, db)

        (
          Aggregate(gbColumns++Seq(Var(WorldBits.columnName)), aggColumns,
            newChild),nonDeterministicInput
        )
      }


      case Limit(offset,count,oldChild) =>{
        limit = true
        val (newChild, nonDeterministicInput) = compileInterleaved(oldChild, db)
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
        val (newChild, nonDeterministicInput) = compileInterleaved(oldChild, db)
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
      case View(_, query, _) =>  compileInterleaved(query, db)




      case ( LeftOuterJoin(_,_,_) | Annotate(_, _) | ProvenanceOf(_) | Recover(_, _) ) =>
        throw new RAException("Interleave mode presently doesn't support LeftOuterJoin")
    }
  }

}
