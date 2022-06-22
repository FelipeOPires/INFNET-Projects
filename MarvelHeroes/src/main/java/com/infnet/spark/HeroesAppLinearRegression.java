package com.infnet.spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HeroesAppLinearRegression {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("HeroesApp")
				.getOrCreate();
		
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		
		//Fazer a leitura do dataset no HDFS e carregá-lo em um objeto da classe Dataset, inferindo o Schema 
		Dataset<Row> data = spark
				.read()
				.option("header", true)
				.option("inferSchema", true)
				.format("csv")
				.load("hdfs://localhost:9000/user/infnet/marvel/charcters_stats.csv");
		
		//MLlib recebe o DataFrame em formato definido. Se trata essencialmente de um DF com duas colunas: 
		//1 - Rótulos (label)
		//2 - Atributos (features) - coluna que consiste de array de valores dos atributos
		Dataset<Row> DataFrame = (data
				.select(col("Total").as("label")
						,col("Intelligence")
						,col("Strength")
						,col("Speed")
						,col("Durability")
						,col("Power")
						,col("Combat")));
		
		//Exibindo o Schema do Dataset
		System.out.println("Schema de dados");
		DataFrame.printSchema();
		
		//Exibindo estatíticas sobre os dados
		System.out.println("\nEstatísticas dos dados");
		DataFrame.describe("Intelligence").show();
		DataFrame.describe("Strength").show();
		DataFrame.describe("Speed").show();
		DataFrame.describe("Durability").show();
		DataFrame.describe("Power").show();
		DataFrame.describe("Combat").show();
		DataFrame.describe("label").show(); // Total
		
		//O DataFrame possui 7 colunas, uma com rótulo e outras 6 com atributos. 
		//Precisamos que esses atributos sejam incluídos em um vetor de atributos (do tipo double) em uma segunda coluna chamada features.
		//Para isso, utilizo o VectorAssembler
		VectorAssembler assembler = (new VectorAssembler()
				.setInputCols(new String[] {"Intelligence"
						,"Strength"
						,"Speed"
						,"Durability"
						,"Power"
						,"Combat"})
						.setOutputCol("features"));
		
		
		//Utilizo o Assembler para transformar o DataFrame em apenas duas colunas: label e features
		Dataset<Row> output = assembler
				.transform(DataFrame)
				.select("label", "features");
		
		//Criando um objeto de modelo de Regressão Linear 
		LinearRegression lr = new LinearRegression();
		
		//Setar o modelo de acordo com os dados
		LinearRegressionModel lrModel = lr.fit(output);
		
		//Imprimindo métricas de desempenho do modelo
		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();

		System.out.println("A acurácia do algoritmo foi de " + Math.round(trainingSummary.r2() * 100) + "%"); //a métrica r2 nos dá uma medida de quanto o modelo consegue representar uma variabilidade dos dados,
														         //ou seja, o quanto o modelo é representativo. 
																//Neste caso o modelo tem representatividade de 99% (0.99 arredondando para cima)
		
		//Exibindo a predição dos dados abaixo referente ao herói Spider-Man
		//"Intelligence" = 88
		//"Strenght" = 55
		//"Speed" = 60
		//"Durability" = 74
		//"Power" = 58
		//"Combat" = 85
		//Total = ???   Quero descobrir qual é o valor total dos atributos deste herói
		
								    //Intelligence, Strenght, Speed, Durability, Power, Combat
		Vector x_new = Vectors.dense(     88,          55,      60,      74,      58,     85);
		System.out.println("\n O Total dos atributos do personagem Spider-Man é de: " + Math.round(lrModel.predict(x_new))); //Utilizei round() para arredondar o valor de 419.99 para 420
		
	}			
}
