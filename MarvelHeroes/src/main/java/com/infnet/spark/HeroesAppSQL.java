package com.infnet.spark;
import org.apache.spark.sql.*;

import java.beans.IntrospectionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HeroesAppSQL {
	
	public static void main(String[] args) throws IntrospectionException {
		
		//Criando a sessão do Spark
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("HeroesApp")
				.getOrCreate();
		
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		
		//Fazer a leitura do dataset  characters.csv, que contém os nomes dos heróis
		Dataset<Row> namesData = spark
				.read()
				.option("header", true)
				.option("inferSchema", true)
				.format("csv")
				.load("hdfs://localhost:9000/user/infnet/marvel/marvel_characters_info.csv");
		
		//Exibindo o Schema do Dataset
		System.out.println("Schema de dados");
		namesData.printSchema();
		
		//Exibindo estatíticas sobre os dados
		System.out.println("\nEstatísticas dos dados");
		namesData.describe("ID").show();
		namesData.describe("Name").show();
		namesData.describe("Alignment").show();
		namesData.describe("Gender").show();
		namesData.describe("EyeColor").show();
		namesData.describe("Race").show();
		namesData.describe("HairColor").show();
		namesData.describe("Publisher").show();
		namesData.describe("SkinColor").show();
		namesData.describe("Height").show();
		namesData.describe("Weight").show();
		
	
		//Criando uma SQL View temporária com os dados
		namesData.createOrReplaceTempView("characters");
		
		//Exibindo a quantidade de heróis da Marvel
		String sqlQueryCharQuantity =  "SELECT Count(Name) as MarvelHeroes"
				+ " FROM characters "
				+ "WHERE Publisher = 'Marvel Comics'";
		
		//Exibindo os nomes dos heróis
		String sqlQueryCharNames =  "SELECT Name, Gender, EyeColor, Race, HairColor "
				+ "FROM characters "
				+ "WHERE Publisher = 'Marvel Comics'";
		
		//Demonstrar valores da média de altura(Height) e peso(Weight) referentes aos dados categóricos da coluna Sexo(Gender)
		String sqlQueryAVGData =  "SELECT Gender, ROUND(AVG(Height),2) as Height, ROUND(AVG(Weight)) as Weight"
				+ " FROM characters "
				+ "WHERE Publisher = 'Marvel Comics' "
				+ "AND Gender != '-' "
				+ "GROUP BY Gender "
				+ "ORDER BY Height DESC";

		//Criando novos datasets que recebem os resultados das queries
		Dataset<Row> sqlDF = spark.sql(sqlQueryCharNames);
		Dataset<Row> sqlDF2 = spark.sql(sqlQueryCharQuantity);	
		Dataset<Row> sqlDF3 = spark.sql(sqlQueryAVGData);	
		
		System.out.println("Quantidade de personagens da Marvel Comics");
		sqlDF2.show();
		
		System.out.println("Personagens Marvel");
		sqlDF.show();
		
		System.out.println("Exibir a média de altura(height) e peso(Weight) dos personagens referente ao Sexo(Gender)");
		sqlDF3.show();	

	}
}