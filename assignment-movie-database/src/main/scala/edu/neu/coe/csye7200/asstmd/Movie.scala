package edu.neu.coe.csye7200.asstmd

import scala.io.Source
import scala.util._

/**
 * This class represents a Movie from the IMDB data file on Kaggle.
 * Although the limitation on 22 fields in a case class has partially gone away, it's still convenient to group the different attributes together into logical classes.
 *
 * Created by scalaprof on 9/12/16.
 *
 * Common questions in this assignment:
 * 1. Where is main method?
 * In most case, you don't need to run main method for assignments.
 * Unit tests are provided to test your implementation.
 * In this assignment, you will find the `object Movie extends App`,
 * the `App` trait can be used to quickly turn objects into executable programs.
 * You can read the official doc of Scala for more details.
 *
 * 2. How to understand the whole program in this assignment?
 * I won't suggest you to understand the whole program in this assignment,
 * there are some advanced features like `implicit` which hasn't been covered in class.
 * You should be able to understand it before midterm.
 * I will suggest you only focus on TO BE IMPLEMENTED fragments in the assignments.
 *
 */
case class Movie(title: String, format: Format, production: Production, reviews: Reviews, director: Principal, actor1: Principal, actor2: Principal, actor3: Principal, genres: Seq[String], plotKeywords: Seq[String], imdb: String)

/**
 * The movie format (including language and duration).
 *
 * @param color       whether filmed in color
 * @param language    the native language of the characters
 * @param aspectRatio the aspect ratio of the film
 * @param duration    its length in minutes
 */
case class Format(color: Boolean, language: String, aspectRatio: Double, duration: Int) {
  override def toString = s"${if (color) "Color" else "B&W"},$language,$aspectRatio,$duration"
}

/**
 * The production: its country, year, and financials
 *
 * @param country   country of origin
 * @param budget    production budget in US dollars
 * @param gross     gross earnings (?)
 * @param titleYear the year the title was registered (?)
 */
case class Production(country: String, budget: Int, gross: Int, titleYear: Int) {
  def isKiwi: Boolean = this match {
    case Production("New Zealand", _, _, _) => true
    case _ => false
  }
}

/**
 * Information about various forms of review, including the content rating.
 */
case class Reviews(imdbScore: Double, facebookLikes: Int, contentRating: Rating, numUsersReview: Int, numUsersVoted: Int, numCriticReviews: Int, totalFacebookLikes: Int)

/**
 * A cast or crew principal
 *
 * @param name          name
 * @param facebookLikes number of FaceBook likes
 */
case class Principal(name: Name, facebookLikes: Int) {
  override def toString = s"$name ($facebookLikes likes)"
}

/**
 * A name of a contributor to the production
 *
 * @param first  first name
 * @param middle middle name or initial
 * @param last   last name
 * @param suffix suffix
 */
case class Name(first: String, middle: Option[String], last: String, suffix: Option[String]) {
  override def toString = s"$first ${middle.getOrElse("")} $last ${suffix.getOrElse("")}}"
}

/**
 * The US rating
 */
case class Rating(code: String, age: Option[Int]) {
  override def toString: String = code + age.map("-" + _).getOrElse("")
}

object Movie extends App {

  implicit object ParsableMovie extends Parsable[Movie] {
    /**
     * Method to yield a Try[Movie] from a String representing a line of input of the movie database file.
     *
     * TODO 11 points.
     *
     * @param w a line of input.
     * @return a Try[Movie]
     */
    def parse(w: String): Try[Movie] = Try {
      val parts = w.split(",").map(_.trim)
      val title = parts(11)
      val color = parts(0) == "Color"
      val language = parts(19)
      val aspectRatio = parts(26).toDouble
      val duration = parts(3).toInt
      val formatParams = List(parts(0), parts(19), parts(26), parts(3))
      val format = Format(formatParams)

      val country = parts(20)
      val budget = parts(22).toInt
      val gross = parts(8).toInt
      val titleYear = parts(23).toInt
      val productionParams = List(parts(20), parts(22), parts(8), parts(23))
      val production = Production(productionParams)

      val imdbScore = parts(25).toDouble
      val facebookLikes = parts(27).toInt
      val contentRating = parts(21)
      val numUsersReview = parts(18).toInt
      val numUsersVoted = parts(12).toInt
      val numCriticReviews = parts(2).toInt
      val totalFacebookLikes = parts(13).toInt
      val reviewsParameters = List(parts(25), parts(27), parts(21), parts(18), parts(12), parts(2), parts(13))
      val reviewsObject = Reviews(reviewsParameters)

      val director = Principal(parts(1), parts(4).toInt)
      val actor1 = Principal(parts(10), parts(7).toInt)
      val actor2 = Principal(parts(6), parts(24).toInt)
      var actor3 = Principal(parts(14), parts(5).toInt)

      val genres = parts(9).split("\\|").toSeq
      val plotKeywords = parts(16).split("\\|").toSeq
      val imdb = parts(17)

      Movie(title, format, production, reviewsObject, director, actor1, actor2, actor3, genres, plotKeywords, imdb)
    }}

  val ingester = new Ingest[Movie]()
  if (args.length > 0) {
    val source = Source.fromFile(args.head)
    val kiwiMovies = for (my <- ingester(source)) yield for (m <- my; if m.production.isKiwi) yield m
    kiwiMovies foreach (_ foreach println)
    source.close()
  }

  /**
   * Form a list from the elements explicitly specified (by position) from the given list
   *
   * @param list    a list of Strings
   * @param indices a variable number of index values for the desired elements
   * @return a list of Strings containing the specified elements in order
   */
  def elements(list: Seq[String], indices: Int*): List[String] = {
    // Hint: form a new list which is consisted by the elements in list in position indices. Int* means array of Int.
    // 6 points
    val result: Seq[String] = {
      indices.map(list(_)).toList
    }
    result.toList
  }

  /**
   * Alternative apply method for the Movie class
   *
   * @param ws a sequence of Strings
   * @return a Movie
   */
  def apply(ws: Seq[String]): Movie = {
    // we ignore faceNumber_in_poster since I have no idea what that means.
    val title = ws(11)
    val format = Format(elements(ws, 0, 19, 26, 3))
    val production = Production(elements(ws, 20, 22, 8, 23))
    val reviews = Reviews(elements(ws, 25, 27, 21, 18, 12, 2, 13))
    val director = Principal(elements(ws, 1, 4))
    val actor1 = Principal(elements(ws, 10, 7))
    val actor2 = Principal(elements(ws, 6, 24))
    val actor3 = Principal(elements(ws, 14, 5))
    val plotKeywords = ws(16).split("""\|""").toList
    val genres = ws(9).split("""\|""").toList
    val imdb = ws(17)
    Movie(title, format, production, reviews, director, actor1, actor2, actor3, genres, plotKeywords, imdb)
  }
}

object Format {
  def apply(params: List[String]): Format = params match {
    case color :: language :: aspectRatio :: duration :: Nil => Format(color == "Color", language, aspectRatio.toDouble, duration.toInt)
    case _ => throw ParseException(s"logic error in Format: $params")
  }
}

object Production {
  def apply(params: List[String]): Production = params match {
    case country :: budget :: gross :: titleYear :: Nil => Production(country, budget.toInt, gross.toInt, titleYear.toInt)
    case _ => throw ParseException(s"logic error in Production: $params")
  }
}

object Reviews {
  def apply(params: List[String]): Reviews = params match {
    case imdbScore :: facebookLikes :: contentRating :: numUsersReview :: numUsersVoted :: numCriticReviews :: totalFacebookLikes :: Nil => Reviews(imdbScore.toDouble, facebookLikes.toInt, Rating(contentRating), numUsersReview.toInt, numUsersVoted.toInt, numCriticReviews.toInt, totalFacebookLikes.toInt)
    case _ => throw ParseException(s"logic error in Reviews: $params")
  }
}

object Name {
  // this regex will not parse all names in the Movie database correctly. Still, it gets most of them.
  private val rName = """^([\p{L}\-\']+\.?)\s*(([\p{L}\-]+\.)\s)?([\p{L}\-\']+\.?)(\s([\p{L}\-]+\.?))?$""".r

  def apply(name: String): Name = (for (ws <- rName.unapplySeq(name)) yield for (w <- ws) yield Option(w))
  match {
    case Some(Seq(Some(first), _, maybeMiddle, Some(last), _, maybeSuffix)) => Name(first, maybeMiddle, last, maybeSuffix)
    case x => throw ParseException(s"parse error in Name: $name (parsed as $x)")
  }
}

object Principal {
  def apply(params: List[String]): Principal = params match {
    case name :: facebookLikes :: Nil => Principal(name, facebookLikes.toInt)
    case _ => throw ParseException(s"logic error in Principal: $params")
  }

  def apply(name: String, facebookLikes: Int): Principal = Principal(Name(name), facebookLikes)
}

object Rating {
  // Hint: This regex matches three patterns: (\w*), (-(\d\d)), (\d\d), for example "PG-13", the first one matches "PG", second one "-13", third one "13".
  private val rRating = """^(\w*)(-(\d\d))?$""".r

  /**
   * Alternative apply method for the Rating class such that a single String is decoded
   *
   * @param s a String made up of a code, optionally followed by a dash and a number, e.g. "R" or "PG-13"
   * @return a Rating
   */
  // Hint: This should similar to apply method in Object Name. The parameter of apply in case match should be same as case class Rating
  // 13 points
  def apply(s: String): Rating = {
    // TO BE IMPLEMENTED
    val parts = s.split("-").toList
    parts match {
      case code :: Nil => Rating(code, None)
      case code :: age :: Nil => Rating(code, Some(age.toInt))
      case _ => throw ParseException(s"Invalid rating format: $s")
    }

  }
}

case class ParseException(w: String) extends Exception(w)