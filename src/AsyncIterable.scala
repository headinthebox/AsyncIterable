/*
 * Copyright 2013 Applied Duality, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 *
 */

package rx.lang.scala

import scala.concurrent.{Future, ExecutionContext}
import scala.async.Async._

object AsyncIterable {

  def apply[T](body: =>()=>Future[Option[T]])(implicit executor : ExecutionContext): AsyncIterable[T] = {
    var b = body
    new AsyncIterable[T] {
      def iterator: AsyncIterator[T] = {
        new AsyncIterator[T] {
          var value: Option[T] = Option.empty
          def next: T = value.get

          def hasNext(): Future[Boolean] = async {
            value = await{ b() }
            !value.isEmpty
          }
        }
      }
    }
  }
}

trait AsyncIterable[+T] {

  def iterator: AsyncIterator[T]

  def filter(predicate: T=>Boolean)(implicit executor : ExecutionContext): AsyncIterable[T] = {
    new AsyncIterable[T] {
      def iterator: AsyncIterator[T] = {
        val iterator = AsyncIterable.this.iterator
        new AsyncIterator[T] {
          def next: T = { iterator.next }
          def hasNext(): Future[Boolean] =  async {
            if(await{ iterator.hasNext() }) {
              if(predicate(iterator.next)) {
                true
              } else {
                await{ iterator.hasNext() }
              }
            } else {
              false
            }
          }
        }
      }
    }
  }
  def map[R](selector: T=>R)(implicit executor : ExecutionContext): AsyncIterable[R] = {
    new AsyncIterable[R] {
      def iterator: AsyncIterator[R] = {
        val iterator = AsyncIterable.this.iterator
        new AsyncIterator[R] {
          def next: R = {
            selector(iterator.next)
          }

          def hasNext(): Future[Boolean] = iterator.hasNext()
        }
      }
    }
  }
  def flatMap[R](selector: T=>AsyncIterable[R])(implicit executor : ExecutionContext): AsyncIterable[R] = {
    new AsyncIterable[R] {
      def iterator: AsyncIterator[R] = {
        val outer = AsyncIterable.this.iterator
        var inner: AsyncIterator[R] = null
        new AsyncIterator[R] {
          def next: R = {
            inner.next
          }

          def hasNext(): Future[Boolean] = async {
            if(inner == null) {
              if(await{ outer.hasNext() }) {
                inner  = selector(outer.next).iterator
                await{ hasNext() }
              } else {
                false
              }
            } else {
              if(await{ inner.hasNext() }) {
                true
              } else {
                inner = null
                await{ hasNext() }
              }
            }
          }
        }
      }
    }
  }

  def forEach(body: T => Unit)(implicit executor : ExecutionContext): Future[Unit] =  {
    def loop(iterator: AsyncIterator[T]): Future[Unit] = async {
      if(await{ iterator.hasNext() }) {
        body(iterator.next)
        await { loop(iterator) }
      } else {

      }
    }
    loop(this.iterator)
  }
}

trait AsyncIterator[+T] {

  def next: T
  def hasNext(): Future[Boolean]

}