import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2; 

public class TheMaraudersPhase3{
	static JavaSparkContext context;
	public static final double MIN_LATITUDE = 40.50f;
	public static final double MAX_LATITUDE = 40.90f;
	public static final double MAX_LONGITUDE = -73.70f;
	public static final double MIN_LONGITUDE = -74.25f;
	public static final double UNIT_SIZE = 0.01f;
	public static final int DAY_RANGE = 1;
	public static final int NUMB_OF_DAYS = 31;
	public static final int NUMB_OF_LATS = (int)((MAX_LATITUDE-MIN_LATITUDE+0.01)/UNIT_SIZE);
	public static final int NUMB_OF_LONGS = (int)Math.abs((MAX_LONGITUDE-MIN_LONGITUDE+0.01)/UNIT_SIZE);
	
	public static final int CELL_COUNT = NUMB_OF_DAYS * NUMB_OF_LATS * NUMB_OF_LONGS;
	
	public static int[][][] coordinates = new int[NUMB_OF_LATS][NUMB_OF_LONGS][NUMB_OF_DAYS];
	
	public static int sum = 0;
	public static float mean = (float) (sum/CELL_COUNT);
	public static float standard_deviation = 0.0f;
	public static HashMap<ArrayList<Integer>,Float> allCoordinateGStatistic = new HashMap<ArrayList<Integer>,Float>();  
	public static Map<Object, String> sortedCoordinates = new HashMap<Object, String>();
	public TheMaraudersPhase3(JavaSparkContext context, String input, String output){
		this.context = context;
		System.out.println("Number of Latitude Coordinates : " + NUMB_OF_LATS);
		System.out.println("Number of Longitude Coordinates : " + NUMB_OF_LONGS);
		System.out.println("Number of Days Coordinates : " + NUMB_OF_DAYS);
		/*
		 * Initializing the lattice coordinates to 0
		 * */
		for(int i=0; i<NUMB_OF_LATS; i++){
			for(int j=0;j<NUMB_OF_LONGS;j++){
				for(int k=0;k<NUMB_OF_DAYS;k++){
					coordinates[i][j][k] =0;
				}
			}
		}
		mapReduce(input,output);
		standard_deviation = (float) calculate_standardDeviation();
		getGStatisticForAllCoordinates();
		sortedCoordinates = sortByValues(allCoordinateGStatistic); 
		/*
		 * Testing with explicit value
		ArrayList<ArrayList<Integer>> neighbours = getNeighbours(24, 26, 27);
		for(int i=0; i< neighbours.size(); i++){
			System.out.println(neighbours.get(i));
		}*/
		writeResultstoFile(output);
	}

	/*
	 * Write Output to specified file
	 * */
	public static void writeResultstoFile(String output) {
		int recordsToPrint = 50;
		FileWriter fileStream = null;
		BufferedWriter out;
		try {
			fileStream = new FileWriter(output);
			out = new BufferedWriter(fileStream);
			int count = 0;
			Set set2 = sortedCoordinates.entrySet();
			Iterator iterator2 = set2.iterator();
			while(iterator2.hasNext() && count < recordsToPrint) {
				Map.Entry pair = (Map.Entry)iterator2.next();
				ArrayList<Integer> key = (ArrayList<Integer>) pair.getKey();
				float original_lat = (float) (MIN_LATITUDE + UNIT_SIZE * key.get(0));
				float original_long = (float) (MIN_LONGITUDE + UNIT_SIZE * key.get(1));
				out.write(original_lat + ", " + original_long + ", " + key.get(2) + ", " +  pair.getValue() + "\n");
				count++;
			}
			out.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Calculate Getis Statistic for all coordinates with value greater than 0 
	 * */
	public static void getGStatisticForAllCoordinates(){
		for(int i=0; i<NUMB_OF_LATS; i++){
			for(int j=0;j<NUMB_OF_LONGS;j++){
				for(int k=0;k<NUMB_OF_DAYS;k++){
					if(coordinates[i][j][k] > 0){
						ArrayList<Integer> coordinate = new ArrayList<Integer>();
						coordinate.add(i);
						coordinate.add(j);
						coordinate.add(k);
						allCoordinateGStatistic.put(coordinate, getGetisStatistic(i, j, k));
					}
				}
			}
		}
	}

	/*
	 * Sort the coordinates by Getis Statitic value
	 * */
	@SuppressWarnings("unchecked")
	public static HashMap<Object, String> sortByValues(HashMap<ArrayList<Integer>, Float> allCoordinateGStatistic2){
		 List<?> list = new LinkedList(allCoordinateGStatistic2.entrySet());
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	               return -((Comparable) ((Map.Entry) (o1)).getValue())
	                  .compareTo(((Map.Entry) (o2)).getValue());
	            }
	       });

	       HashMap sortedHashMap = new LinkedHashMap();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	              Map.Entry entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       return sortedHashMap;
	}

	/*
	 * Map reduce task to form the lattice. 
	 * Lattice is bounded by MAX_LATITUDE, MIN_LATITUDE, MAX_LONGITUDE AND MIN_LONGITUDE
	 * Lattice coordinates values are scaled to start from 0.
	 * */	
	public static void mapReduce(String inputFilePath, String outputFilePath){
		JavaRDD<String> inputFileData = context.textFile(inputFilePath);
		JavaPairRDD<String,Integer> map = inputFileData.mapToPair(
				new PairFunction<String,String,Integer>(){	
					public Tuple2<String, Integer> call(String arg0) throws Exception{
						//System.out.println("Input line : " + arg0);
						if(!arg0.split(",")[0].equals("VendorID")){
							String date = arg0.split(",")[1].split(" ")[0].split("-")[2];
							double longitutde = Double.parseDouble(arg0.split(",")[5]);
							double latitude = Double.parseDouble(arg0.split(",")[6]);
							
							double LATRange = MAX_LATITUDE-MIN_LATITUDE;
							double LONGRange = MAX_LONGITUDE-MIN_LONGITUDE;
							
							if(latitude<=MAX_LATITUDE && latitude>=MIN_LATITUDE && longitutde<=MAX_LONGITUDE && longitutde>=MIN_LONGITUDE){
								int Derivedlat = (int)((latitude-MIN_LATITUDE)/UNIT_SIZE);
								int Derivedlong = (int)((longitutde-MIN_LONGITUDE)/UNIT_SIZE);
								int Deriveddate = Integer.parseInt(date)-1;
								/*if(Derivedlong >= 55){
									System.out.println("Longitude "+ longitutde);
									System.out.println("derived Longitude "+ Derivedlong);
									System.out.println(" Is greater" + (longitutde < MAX_LONGITUDE));
								}*/
								return new Tuple2<String, Integer>(Derivedlat+","+Derivedlong+","+Deriveddate, 1);
							} 
							else{
								return new Tuple2<String,Integer>("",0);
							}
						}
						else{
							return new Tuple2<String,Integer>("",0);
						}
					}
				}).reduceByKey(
						new Function2<Integer,Integer,Integer>(){
							public Integer call(Integer v1, Integer v2) throws Exception{
								return v1+v2;
							}
						});
		Map<String,Integer> attributeMap = map.collectAsMap(); 
		for(Map.Entry<String, Integer> row : attributeMap.entrySet()){
			if(row.getKey()==null || row.getKey().equals(""))
				continue;
			String[] temp = row.getKey().split(",");
			int x = Integer.parseInt(temp[0]);
			int y = Integer.parseInt(temp[1]);
			int z = Integer.parseInt(temp[2]);
			//System.out.println(x + ", "+ y + "," + z);
			coordinates[x][y][z] = row.getValue();
			sum += coordinates[x][y][z];
		}
		mean = (float) sum/CELL_COUNT;
		//System.out.println("sum : " + sum);
		//System.out.println("CELL "+ CELL_COUNT);
		System.out.println("Mean : " + mean);
		//map.saveAsTextFile(outputFilePath);
	}

	public static double calculate_standardDeviation(){
		double sd = 0.0f;
		long square_value = 0;
		for(int i=0; i<NUMB_OF_LATS; i++){
			for(int j=0;j<NUMB_OF_LONGS;j++){
				for(int k=0;k<NUMB_OF_DAYS;k++){
					int product = coordinates[i][j][k] * coordinates[i][j][k];
					//System.out.println("coordinates  : " + coordinates[i][j][k]);
					square_value += product;
					//System.out.println("Inside : " + square_value);
				}
			}
		}
		//System.out.println("Square value " + square_value);
		sd = (double) Math.sqrt((square_value/ CELL_COUNT) - (mean * mean));
		System.out.println("Standard Deviation :"  + sd);
		return sd;
	}

	/*
	 * Get Neighbors of a particular coordinate
	 * A Coordinate is a neighbor only when it has some weight/ value, i.e, it is greater than 0
	 * */
	public static ArrayList<ArrayList<Integer>> getNeighbours(int latitude, int longitude, int date){
		int neighbor_coordinates[] = {-1,0,1};	
		ArrayList<ArrayList<Integer>> neighbours = new ArrayList<ArrayList<Integer>>();
		for(int i : neighbor_coordinates){
			for(int j : neighbor_coordinates){
				for(int k : neighbor_coordinates){
					ArrayList<Integer> coordinate= new ArrayList<Integer>();
					int newLatitude = latitude + i;
					int newLongitude = longitude + j;
					int newDate = date + k;
					if(newLatitude >=0 && newLatitude < NUMB_OF_LATS && newLongitude>=0 && newLongitude < NUMB_OF_LONGS 
							&& newDate >=0 && newDate < NUMB_OF_DAYS){
						//System.out.println(coordinates[newLatitude][newLongitude][newDate]);
						if(coordinates[newLatitude][newLongitude][newDate] > 0){
							//System.out.println(" True !!");
							coordinate.add(newLatitude);
							coordinate.add(newLongitude);
							coordinate.add(newDate);
							neighbours.add(coordinate);
						}else{
							//System.out.println("Zeroo " + newLatitude + " , "+ newLongitude + ", "+ newDate + 
								//	" value : " + coordinates[newLatitude][newLongitude][newDate] + 
									//" Exists : " + (coordinates[newLatitude][newLongitude][newDate] > 0) );
						}
					}else{
						//System.out.println("Skipping " + newLatitude + " , "+ newLongitude + ", "+ newDate);
					}
				}
			}
		}
		return neighbours;
	}
	
	/*
	 * Getis Statistic computation using formula specified in the problem statement
	 * */
	public static float getGetisStatistic(int latitude, int longitude, int date){
		ArrayList<ArrayList<Integer>> neighbors = getNeighbours(latitude, longitude, date);
		int neighborsSize = neighbors.size();
		int numerator_sum = 0;
		float numerator = 0.0f;
		double denominator_sum = 0.0f;
		float denominator = 0.0f;
		for(int i=0; i< neighbors.size(); i++){
			ArrayList<Integer> coordinate = neighbors.get(i);
			numerator_sum += coordinates[coordinate.get(0)][coordinate.get(1)][coordinate.get(2)];
		}
		numerator = numerator_sum - ( mean * neighborsSize); 
		denominator_sum = (double) ( ( CELL_COUNT * neighborsSize ) - (neighborsSize * neighborsSize) ) / (CELL_COUNT -1);
		denominator = (float) (standard_deviation * Math.sqrt(denominator_sum));
		return numerator/denominator;
	}
	
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("Phase3");
		JavaSparkContext context = new JavaSparkContext(conf);
		//String input = "/home/saiteja/Downloads/yellow_tripdata_2015-01.csv";
		//String output = "/home/saiteja/output.txt";
		new TheMaraudersPhase3(context, args[0], args[1]);
	}
}