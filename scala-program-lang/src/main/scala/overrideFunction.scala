class Arithmetic{
  def add(a:Int, b:Int){
    var sum = a+b
    println(sum)
  }
  def add(a:Int, b:Int, c:Int){
    var sum = a+b+c
    println(sum)
  }
  def add(a:Double, b:Double):Unit={
    var sum = a+b
    println(sum)
  }
}

object overrideFunction {
  def main(args: Array[String]) {
    var a = new Arithmetic();
    a.add(10, 10);
    a.add(10, 10, 10);
    a.add(10.0, 20.0)
  }
}

