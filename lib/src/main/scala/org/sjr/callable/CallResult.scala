package org.sjr.callable

case class CallForDataResult[T](
                                 /**
                                  * The records the store procedure returns. Empty if nothing returned
                                  */
                                 records: Seq[T],

                                 /**
                                  * The Out/Inout params values set by the stored procedure.
                                  *
                                  * Key is the param index (1-based)
                                  *
                                  * If the value set by stored procedure is null, there won't be an entry here
                                  */
                                 outValues: Map[Int, Any])


case class CallToUpdateResult(/**
                                * @see [[CallForDataResult]]
                                */
                               outValues: Map[Int, Any])
