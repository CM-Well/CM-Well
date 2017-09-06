# Internal HTTP client API
In order to decouple http-client code from specific implementations,
we add a façade layer which separate actual http client implementation from our usage.
The reason we did it, is to not be bound to external libraries, which may not progress fast enough,
and allow us to replace those libraries at ease, without re-writing large chunks of code.

API is defined and implemented in `cmwell.util.http` package:

```scala
object SimpleHttpClient {

  def ws[T : SimpleMessageHandler](uri: String,
         initiationMessage: T,
         subprotocol: Option[String] = None,
         queryParams: Seq[(String,String)] = Nil,
         headers: Seq[(String,String)] = Nil)(react: T => Option[T])
        (implicit ec: ExecutionContext,
         as: ActorSystem = this.sys,
         mat: Materializer = this.mat) = ...

  def get[T : SimpleResponseHandler](uri: String,
          queryParams: Seq[(String,String)] = Nil,
          headers: Seq[(String,String)] = Nil)
         (implicit ec: ExecutionContext,
          as: ActorSystem = this.sys,
          mat: Materializer = this.mat) = ...

  def put[T : SimpleResponseHandler](uri: String,
          body: Body,
          contentType: Option[String] = None,
          queryParams: Seq[(String,String)] = Nil,
          headers: Seq[(String,String)] = Nil)
         (implicit ec: ExecutionContext,
          as: ActorSystem = this.sys,
          mat: Materializer = this.mat) = ...

  def post[T : SimpleResponseHandler](uri: String,
           body: Body,
           contentType: Option[String] = None,
           queryParams: Seq[(String,String)] = Nil,
           headers: Seq[(String,String)] = Nil)
          (implicit ec: ExecutionContext,
           as: ActorSystem = this.sys,
           mat: Materializer = this.mat) = ...

  def delete[T : SimpleResponseHandler](uri: String,
             queryParams: Seq[(String,String)] = Nil,
             headers: Seq[(String,String)] = Nil)
            (implicit ec: ExecutionContext,
             as: ActorSystem = this.sys,
             mat: Materializer = this.mat) = ...

}
```

We use SimpleResponseHandler typeclass to handle the output type generically,
according to what the user wants (think of getting the reponse as `InputStream`,`String`,`ByteArray`, or any other type you may want).
We also use `implicit`s for `Body` to enable getting the request payload in every possible type (à la magnet pattern).

Websockets API also use a typeclass `SimpleMessageHandler` to handle any type as messages.

Configuration is defined in reference conf under `cmwell.util.http`, and it'll pick up any akka configuration for default `ActorSystem` & `Materializer`.
These defaults are only applied if users does not supply instances of their own.

There's a blog post describing this in more detail at: [blog.hochgi.com/2017/07/a-tale-of-bad-framework-choices.html](http://hochgi.blogspot.co.il/2017/07/a-tale-of-bad-framework-choices.html)
