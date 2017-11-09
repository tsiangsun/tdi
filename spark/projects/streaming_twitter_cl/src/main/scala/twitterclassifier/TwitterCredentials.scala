package twitterclassifier

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import scala.util.parsing.json.JSON

object TwitterCredentials {
  def parseTwitterCredentials(twitterCredentialsFile: String) = {
    val source = scala.io.Source.fromFile(twitterCredentialsFile)
    val lines = try source.mkString finally source.close()
    val json = JSON.parseFull(lines).get.asInstanceOf[Map[String, String]]
    System.setProperty("twitter4j.oauth.consumerKey", json("api_key"))
    System.setProperty("twitter4j.oauth.consumerSecret", json("api_secret"))
    System.setProperty("twitter4j.oauth.accessToken", json("access_token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", json("access_token_secret"))
    Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  }
}
