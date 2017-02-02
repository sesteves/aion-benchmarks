/**
  * Created by Sergio on 01/02/2017.
  */

import scala.math.{ Pi, cos, sin, cosh, sinh, abs }

/**
  *  Cooley-Tukey FFT
  */
object FFT {

  case class Complex(re: Double, im: Double = 0) {
    def +(x: Complex): Complex = Complex(re + x.re, im + x.im)

    def -(x: Complex): Complex = Complex(re - x.re, im - x.im)

    def *(x: Double): Complex = Complex(re * x, im * x)

    def *(x: Complex): Complex = Complex(re * x.re - im * x.im, re * x.im + im * x.re)

    def /(x: Double): Complex = Complex(re / x, im / x)

    override def toString(): String = {
      val a = "%1.3f" format re
      val b = "%1.3f" format abs(im)
      (a, b) match {
        case (_, "0.000") => a
        case ("0.000", _) => b + "i"
        case (_, _) if im > 0 => a + " + " + b + "i"
        case (_, _) => a + " - " + b + "i"
      }
    }
  }

  def real(re: Double) = Complex(re, 0)

  def exp(c: Complex): Complex = {
    val r = (cosh(c.re) + sinh(c.re))
    Complex(cos(c.im), sin(c.im)) * r
  }

  def _fft(cSeq: Seq[Complex], direction: Complex, scalar: Int): Seq[Complex] = {
    if (cSeq.length == 1) {
      return cSeq
    }
    val n = cSeq.length
    assume(n % 2 == 0, "The Cooley-Tukey FFT algorithm only works when the length of the input is even.")

    val evenOddPairs = cSeq.grouped(2).toSeq
    val evens = _fft(evenOddPairs map (_ (0)), direction, scalar)
    val odds = _fft(evenOddPairs map (_ (1)), direction, scalar)

    def leftRightPair(k: Int): Pair[Complex, Complex] = {
      val base = evens(k) / scalar
      val offset = exp(direction * (Pi * k / n)) * odds(k) / scalar
      (base + offset, base - offset)
    }

    val pairs = (0 until n / 2) map leftRightPair
    val left = pairs map (_._1)
    val right = pairs map (_._2)
    left ++ right
  }

  def fft(cSeq: Seq[Complex]): Seq[Complex] = _fft(cSeq, Complex(0, 2), 1)

  def rfft(cSeq: Seq[Complex]): Seq[Complex] = _fft(cSeq, Complex(0, -2), 2)
}