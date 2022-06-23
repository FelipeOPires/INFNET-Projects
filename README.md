# MarvelHeroes
Projeto realizado no curso de Banco de Dados (BI e Big Data) referente ao bloco 5, que contém as disciplinas: 

- Desenvolvimento de soluções Big Data com Apache Spark
- Implantação de soluções Big Data com Hadoop
- Projeto de Bloco - Infraestrutura para Big Data - Volume

## Objetivo

O projeto tem por objetivo responder algumas questões sobre o universo Marvel, como por
exemplo, quantos personagens existem e quais são seus atributos e dados pessoais. O tema foi
escolhido devido a popularidade deste assunto e por eu ser um grande fã da franquia. Para
realizar este objetivo utilizo o Apache Spark, Apache Hadoop, conceitos de Machine Learning
e a linguagem Java para desenvolvimento da aplicação.

## Datasets Utilizados

https://www.kaggle.com/dannielr/marvel-superheroes?select=characters.csv

| Arquivo  | charcters_stats.csv (20.8 kB) |
| ------------- | ------------- |
| Colunas       |  Detalhamento |
| Name  | Alignment  |
| Intelligence  | Strenght  |
| Speed  | Durability  |
| Power  | Combat  |
| Total  | -  |

As colunas Name e Alignment tratam-se de dados categóricos.
As colunas Intelligence, Strenght, Speed, Durability, Power, Combat e Total são dados numéricos.

| Arquivo  | LR_Characters_stats.csv (21.5 kB) |
| ------------- | ------------- |
| Colunas       |  Detalhamento |
| Name  | Alignment  |
| Validation | Intelligence  |
| Strenght | Speed  |
| Durability | Power | 
| Combat  |Total  |

As colunas Name e Alignment tratam-se de dados categóricos.
As colunas Validation, Intelligence, Strenght, Speed, Durability, Power, Combat e Total são dados numéricos.

| Arquivo  | Marvel_characters_info (45.1 kB) |
| ------------- | ------------- |
| Colunas       |  Detalhamento |
| ID  | Name |
| Alignment | Gender |
| EyeColor | Race  |
| HairColor | Publisher | 
| SkinColor | Height  |
| Weight | - |

As colunas Name e Alignment, Gender, EyeColor, Race, Haior Color, Publisher e SkinColor tratam-se de dados categóricos.
As colunas ID, Height e Weight são dados numéricos.

Os dados dos datasets utilizados neste projeto são estruturados, ou seja, são organizados e representados em uma estrutura previamente planejada.

## Metodologia

### Pré-requisitos:  

1. Criar um diretório no HDFS para o Dataset: ```hdfs dfs -mkdir /user/infnet/marvel```  
2. Acessar o diretório de origem do dataset: ``cd /home/infnet/datasets/Marvel``
3. Copiar os arquivos .csv do dataset para o HDFS:
```
hdfs dfs -put charcters_stats.csv /user/infnet/marvel/
hdfs dfs -put LR_charcters_stats.csv /user/infnet/marvel/
hdfs dfs -put marvel_characters_info.csv /user/infnet/marvel/
```
![image](https://user-images.githubusercontent.com/97707889/175179022-1b7b7221-a6c2-4773-8e48-9c23d98293c9.png)

4. Configurar o pom.xml do projeto MarvelHeroes para utilização do spark, spark
MLlib e Hadoop.

### pom.xml
```
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.infnet.spark</groupId>
  <artifactId>HeroesApp</artifactId>
  <version>0.0.1-SNAPSHOT</version>

 	<dependencies>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-core_2.11</artifactId>
			<version>2.1.0</version>
		</dependency>
			
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql_2.11</artifactId>
			<version>2.1.0</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-client</artifactId>
			<version>2.6.0</version>
		</dependency>
	</dependencies>
	
		<properties>
		<maven.compiler.source>1.8</maven.compiler.source>
		<maven.compiler.target>1.8</maven.compiler.target>
	</properties>
	
</project>
```

## Questionamentos e hipóteses sobre os datasets

- Quantos/quem são os personagens da Marvel?  
  Resposta: Os personagens da Marvel são:
   A
   B
   C
   O total de personagens é de x;  
   
   Atuação: Para realizar a contagem de quantos e quais são os heróis da Marvel que constam no dataset Marvel_characters_info.csv, foi necessário utilizar o apache spark, spark e spark query, além do apache hadoop para salvar o arquivo .csv no HDFS. Criei uma variável do tipo String que recebe uma consulta SQL e um Dataframe que receba como parâmetro o resultado. A classe referente a essa análise é a HeroesAppSQL.java.
   
  | Resultado |
  |-|
  | ![image](https://user-images.githubusercontent.com/97707889/175179858-72c2a3cf-d491-4471-bc71-36eeeb921773.png) | 

- Qual é a média de peso e altura dos personagens referente ao sexo?  
  Resposta: Sexo(Gender) masculino, média de altura (Height) e peso (Weight);
            Sexo(Gender) feminino, média de altura (Height) e peso (Weight);
   
  | Resultado |
  |-| 
  | ![image](https://user-images.githubusercontent.com/97707889/175179964-5c2cd514-5ff4-44b8-b3b5-bc98557da784.png) |
   
  Atuação: Para realizar esta análise, utilizei o dataset Marvel_characters_info.csv e realizei o mesmo procedimento do item anterior, criei uma variável do tipo String que recebe uma consulta SQL e um Dataframe que receba como parâmetro essa consulta. A classe referente a essa análise é a HeroesAppSQL.java.     

- Qual o valor total de atributos de um personagem?  
  Resposta: O total de atributos do personagem x é de y.
  
  Atuação: Para realizar esta análise, utilizei o dataset charcters_stats.csv e a biblioteca MlLib, assim como um algoritmo de regressão linear para que pudesse prever o valor total de atributos de determinado personagem utilizando Machine Learning. A classe referente a essa análise é a HeroesappLinearRegression.java.
  
  | Resultado |
  |-| 
  | ![image](https://user-images.githubusercontent.com/97707889/175180577-b28babe0-982a-455e-a034-894b75b9de7d.png) |  
    
- Qual é o alinhamento dos heróis? (good ou bad)  
  Resposta esperada: A predição para heróis com alinhamento bom (good), é de x resultados.  
  
  ```
  Alinhamento 0 -> Predição 0 = probabilidade 
  ```
  Atuação: Para realizar esta análise, utilizei o dataset LR_charcters_stats.csv e a biblioteca MlLib, Pipeline, divisão do dataset entre treino e teste assim como um algoritmo de regressão logística para que pudesse prever os valores de alinhamento (Alignment) dos personagens com Machine Learning. A classe referente a essa análise é a HeroesappLinearRegression.java.

  | Resultado |
  |-| 
  | ![image](https://user-images.githubusercontent.com/97707889/175180639-185ee466-b1bc-4987-8ef7-3a66cbbfc51e.png) |  


## Conclusão

- O algoritmo utilizado na classe HeroesAppLogisticRregression.java, referente ao questionamento “qual seria o alinhamento dos heróis (bom ou mau)?” demonstrou uma acurácia de 100%. O código ao ser executado sempre exibe corretamente a quantidade de heróis com alinhamento “bom” ou “mau.  

- O código criado para responder à pergunta “qual o total de atributos de um personagem?” demonstrou ser assertivo. Há uma acurácia de 100% ao prever quais são os valores totais de atributos do personagem Spider-Man, dado outros atributos como base.  

- A classe HeroesAPPSQL.java foi criada de forma a exibir os resultados dos questionamentos “Quem são os heróis de Marvel? E sua quantidade?” e “Qual é a média de peso e altura dos personagens referente ao sexo?”. As queries utilizadas neste cenário são simples, porém demonstram os resultados esperados.  

Foram necessárias diversas horas de estudos para compreender e montar o cenário do projeto, assim como o auxílio dos professores em algumas ocasiões. Com certeza foi uma experiência de grande valia pessoal.  

