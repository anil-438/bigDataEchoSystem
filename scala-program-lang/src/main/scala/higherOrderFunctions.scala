object higherOrderFunctions {
  // Function as Parameter in a Function
  def main(args: Array[String]) = {
    functionExample(25, multiplyBy3) // Passing a function as parameter
  }

  def functionExample(a: Int, f: Int => AnyVal): Unit = {
    println(f(a)) // Calling that function
  }

  def multiplyBy3(a: Int): Int = {
    a * 2
  }

  // Function Composition
  var result = multiplyBy2(add2(10)) // Function composition
  println(result)

  def add2(a: Int): Int = {
    a + 2
  }

  def multiplyBy2(a: Int): Int = {
    a * 2
  }

  /*Anonymous (lambda) Function
  You can create anonymous function either by using => (rocket) or _ (underscore) wild card in scala.
 */
  def lambda(args: Array[String]) = {
    var result1 = (a: Int, b: Int) => a + b // Anonymous function by using => (rocket)
    var result2 = (_: Int) + (_: Int) // Anonymous function by using _ (underscore) wild card
    println(result1(10, 10))
    println(result2(10, 10))
  }

  //Function Currying
  def add(a: Int)(b: Int) = {
    a + b
  }

  def Currying(args: Array[String]) = {
    var result = add(10)(10)
    println("10 + 10 = " + result)
    var addIt = add(10) _
    var result2 = addIt(3)
    println("10 + 3 = " + result2)
  }

  //Nested Functions
  def add(a: Int, b: Int, c: Int) = {
    def add2(x: Int, y: Int) = {
      x + y
    }

    add2(a, add2(b, c))
  }

  def Nested(args: Array[String]) = {
    var result = add(10, 10, 10)
    println(result)
  }

  //Function with Variable Length Parameters
  def add(args: Int*) = {
    var sum = 0;
    for (a <- args) sum += a
    sum
  }

  var sum = add(1, 2, 3, 4, 5, 6, 7, 8, 9);
  println(sum)
}

