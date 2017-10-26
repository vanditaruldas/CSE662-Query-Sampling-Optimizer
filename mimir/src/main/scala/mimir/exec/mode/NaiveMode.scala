package mimir.exec.mode

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.ctables.{CTAnalyzer, CTables}
import mimir.exec.result.{ResultIterator, SampleResultIterator}
import mimir.models.Model
import mimir.optimizer.operator.{InlineProjections, ProjectRedundantColumns, PushdownSelections}
import mimir.provenance.Provenance

import scala.collection.mutable.ArrayBuffer

class NaiveMode (seeds: Seq[Long] = (0l until 10l).toSeq)
  extends CompileMode[SampleResultIterator]
    with LazyLogging{

  type MetadataT =
    (
      Set[String],   // Nondeterministic column set
        Seq[String]    // Provenance columns
      )


  def compileNaive(query: Operator, db: Database):(Operator, Set[String]) =
  {
    val sampleShards =
      (0 until seeds.size).map { i =>
          ReplaceMissingLensWithSampler(query,db.models.get(_),i)_1

      }
    OperatorUtils.makeUnion(sampleShards)
//    var nonDeterministic:Set[String] = Set()
    val (compiled, nonDeterministicColumns) = ReplaceMissingLensWithSampler(query,db.models.get(_),0)

    (OperatorUtils.makeUnion(sampleShards), nonDeterministicColumns)
  }

  def rewrite(db: Database, queryRaw: Operator): (Operator, Seq[String], MetadataT) =
  {
    var query = queryRaw

    val (withProvenance, provenanceCols) = Provenance.compile(query)
    query = withProvenance

    val (compiled, nonDeterministicColumns) = compileNaive(query,db)
    query = compiled
    //println(compiled);
    query = db.views.resolve(query)

    (
      query,
      query.columnNames,
      (nonDeterministicColumns, provenanceCols)
    )
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



  def ReplaceMissingLensWithSampler(query: Operator, models: (String => Model), shift: Int): (Operator, Set[String]) =
  {
    // Check for a shortcut opportunity... if the expression is deterministic, we're done!
    if(CTables.isDeterministic(query)){
      return (
        query.addColumn(
          WorldBits.columnName -> IntPrimitive(1<<shift)
        ),
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
        val (newChild, nonDeterministicInput) = ReplaceMissingLensWithSampler(oldChild, models,shift)
        val (
          newColumns,
          nonDeterministicOutputs
          ):(Seq[ProjectArg], Seq[Set[String]]) = columns.map { col =>
          if(!CTables.isDeterministic(col.expression)){

            var nullMap:Map[String,Expression] = Map()
            (ProjectArg(col.name,
            CTAnalyzer.compileSample(
              Eval.inline(col.expression, nullMap),
              IntPrimitive(seeds(shift)),
              models
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
        val (newChild, nonDeterministicInput) = ReplaceMissingLensWithSampler(oldChild, models,shift)

        ( Select(condition, newChild), nonDeterministicInput )
      }

      case Join(lhsOldChild, rhsOldChild) => {
        val (lhsNewChild, lhsNonDeterministicInput) = ReplaceMissingLensWithSampler(lhsOldChild, models,shift)
        val (rhsNewChild, rhsNonDeterministicInput) = ReplaceMissingLensWithSampler(rhsOldChild, models,shift)


        (OperatorUtils.makeSafeJoin(lhsNewChild,rhsNewChild)_1, lhsNonDeterministicInput ++ rhsNonDeterministicInput)
      }

      case Union(lhsOldChild, rhsOldChild) => {
        val (lhsNewChild, lhsNonDeterministicInput) = ReplaceMissingLensWithSampler(lhsOldChild, models,shift)
        val (rhsNewChild, rhsNonDeterministicInput) = ReplaceMissingLensWithSampler(rhsOldChild, models,shift)


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
        val (newChild, nonDeterministicInput) = ReplaceMissingLensWithSampler(oldChild, models,shift)

        (
            Aggregate(gbColumns++Seq(Var(WorldBits.columnName)), aggColumns,
              newChild),nonDeterministicInput
          )

        }
      case Limit(offset,count,oldChild) =>{
        val (newChild, nonDeterministicInput) = ReplaceMissingLensWithSampler(oldChild, models,shift)
        (
          Limit(offset,count,newChild),nonDeterministicInput
        )

      }

      case Sort(sortCols,oldChild)=>{
        val (newChild, nonDeterministicInput) = ReplaceMissingLensWithSampler(oldChild, models,shift)
        (
          Sort(sortCols,newChild),nonDeterministicInput
        )

      }



      // We don't handle materialized tuple bundles (at the moment)
      // so give up and drop the view.
      case View(_, query, _) =>  ReplaceMissingLensWithSampler(query, models,shift)

      case ( LeftOuterJoin(_,_,_) | Annotate(_, _) | ProvenanceOf(_) | Recover(_, _) ) =>
        throw new RAException("NaiveMode presently doesn't support LeftOuterJoin")
    }
  }

}
