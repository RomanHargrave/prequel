package net.noerd.prequel

/**
  * Created by giovannicaruso on 02/09/16.
  */
trait ProcedureParameter

case class ParameterOut(parameterType: Int) extends ProcedureParameter
object ParameterOut

case class ParameterIn(value: Option[Any], parameterType: Int) extends ProcedureParameter
object ParameterIn

case class ParameterInOut(value: Option[Any], parameterType: Int) extends ProcedureParameter
object ParameterInOut

case class ProcedureParameterSet(parameters: List[ProcedureParameter])
object ProcedureParameterSet

