package com.infnet.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HeroesAppLogisticRegression {
	
	public static void main(String[] args) {
		
		//Instanciando a Spark Session
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("HeroesApp")
				.getOrCreate();
		
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		
		LogisticRegression(spark);
		
		spark.stop();	
}

private static void LogisticRegression(SparkSession spark) {
	
			//Definindo o Schema de dados com StructType
			StructType HeroesSchema = new StructType()	
						.add("Name", "string")
						.add("Alignment", "string")
						.add("Validation", "integer")
						.add("Intelligence", "integer")
						.add("Strength", "integer")
						.add("Speed", "integer")
						.add("Durability", "integer")
						.add("Power", "integer")
						.add("Combat", "integer")
						.add("Total", "integer");		
			
		//Fazer a leitura do dataset no HDFS e carregá-lo em um objeto da classe Dataset, inferindo o Schema 
		Dataset<Row> data = spark
				.read()
				.option("header", true)
				.schema(HeroesSchema)
				.format("csv")
				.load("hdfs://localhost:9000/user/infnet/marvel/LR_charcters_stats.csv");
		
		//Exibir schema inferido (HeroesSchema)
		//Exibindo o Schema do Dataset
		System.out.println("Schema de dados inferido");
		data.printSchema();
		
		//Para que fosse possível realizar a Regressão Logística, foi necessário incluir a coluna 'Validation' no Dataset 
		//para que servisse de comparação quanto ao valor de 'Aligment' dos heróis.
		
		//MLlib recebe o DataFrame em formato definido. Se trata essencialmente de um DF com duas colunas: 
		//1 - Rótulos (label)
		//2 - Atributos (features) - coluna que consiste de array de valores dos atributos
		Dataset<Row> DataFrame = (data
				.select(col("Validation").as("label")
						,col("Alignment")
						,col("Intelligence")
						,col("Strength")
						,col("Speed")
						,col("Durability")
						,col("Power")
						,col("Combat")
						,col("Total")));
				
		//Exibindo estatíticas sobre os dados
		System.out.println("\nEstatísticas dos dados");
		DataFrame.describe("Alignment").show();
		DataFrame.describe("label").show();            //Validation
		DataFrame.describe("Intelligence").show();
		DataFrame.describe("Strength").show();
		DataFrame.describe("Speed").show();
		DataFrame.describe("Durability").show();
		DataFrame.describe("Power").show();
		DataFrame.describe("Combat").show();
		DataFrame.describe("Total").show();         // Total
		
		//É necessário codificar os dados categóricos (String) em numéricos para que o Spark possa importá-los
		//Para isso, transformo as categorias em valores numéricos, ou índices (index)		
		StringIndexer alignmentIndexer = new StringIndexer()
				.setInputCol("Alignment")
				.setOutputCol("AlignmentIndex");
		
		OneHotEncoder alignmentEncoder = new OneHotEncoder()
				.setInputCol("AlignmentIndex")
				.setOutputCol("AlignmentVec");
		
		//O DataFrame possui 9 colunas, uma com rótulo e outras 8 com atributos. 
		//Precisamos que esses atributos sejam incluídos em um vetor de atributos (do tipo double) em uma segunda coluna chamada features.
		//Para isso, utilizo o VectorAssembler
		VectorAssembler assembler = (new VectorAssembler()
				.setInputCols(new String[] {"AlignmentVec"
						,"Intelligence"
						,"Strength"
						,"Speed"
						,"Durability"
						,"Power"
						,"Combat"
						,"Total"}))
				.setOutputCol("features");
		
		//Dividindo os dados do dataset
		Dataset<Row>[] split = DataFrame.randomSplit(new double[] {0.7, 0.3});
		Dataset<Row> train = split[0];
		Dataset<Row> test = split[1];
		
		//Criando o Pipeline
		LogisticRegression lr = new LogisticRegression();
		
		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[]
						{alignmentIndexer
								,alignmentEncoder
								,assembler
								,lr});	
		
		//Inserir o pipeline para treino
		PipelineModel model = pipeline.fit(train);
		
		//Obter resultados em teste
		Dataset<Row> predictions = model.transform(test);
		

		//Modelo de Avaliação
		double tp = 0.0; //Positivo Verdadeiro
		double tn = 0.0; //Negativo Verdadeiro
		double fp = 0.0; //Falso Positivo
		double fn = 0.0; //Falso Negativo
		
		System.out.println("Alinhamento dos heróis: Número 1 = 'Good' e númber 0 = 'Bad'");
		
		for (Row r : predictions.select("label", "prediction", "probability").collectAsList()) {
		System.out.println("Alinhamento:" + r.get(0) 
		+ ", Predição:" + r.get(1) + ") --> probabilidade=" + r.get(2));
		
		if(r.get(0).equals(1) && r.get(1).equals(1.0)) tp = tp + 1.0;
		else if(r.get(0).equals(0) && r.get(1).equals(0.0)) tn = tn + 1.0;
		else if(r.get(0).equals(0) && r.get(1).equals(1.0)) fp = fp + 1.0;
		else if(r.get(0).equals(1) && r.get(1).equals(0.0)) fn = fn + 1.0;
		else System.out.println("Erro");
		}
		
		double total = (tp+tn+fp+fn); // Deve ser igual a test.count();
		System.out.println("\nResultados de predição dos dados");
		System.out.println("\nPositivos Verdadeiros, ou seja, alinhamento 'Good' : " + tp + " de " + total + " (" +tp/total+ ");");
		System.out.println("\nNegativos Verdadeiros, ou seja, alinhamento 'Bad' : " + tn + " de " + total + " (" +tn/total+ ");");
		System.out.println("\nFalsos Positvos: " + fp + " de " + total + " (" +fp/total+ ");");
		System.out.println("\nFalsos Negativos: " + tn + " de " + total + " (" +fn/total+ ");");
		
		System.out.println("\nAcurácia de: " + Math.round((tp+tn)/(total) *100) + "%, no conjunto de testes que possui " + test.count() + " registros;");
		
		//Exibir as 10 primeiras linhas do dataset de teste após a execução do modelo de treino
		System.out.println("\nDataset resultante após execução do modelo de treino");
		predictions.show(10);
	}
}
