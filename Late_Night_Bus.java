import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.*;

public class Late_Night_Bus {
	/*
	 * 최종 결과물의 형태가 XXXXX이며, 거리는 XXXX이다 이기 때문에 key, value가 모두 Text로 들어가야 하는데,
	 * Mapper와 Combiner 클래스에서 OutputCollector의 데이터 타입을 (LongWritable, Text)로 지정을 하였더니
	 * OutputKeyClass, ValueClass가 main 메소드에서 지정한 것과 다르다고 오류가 발생했다.
	 * 따라서 Mapper, Combiner, Reducer의 output Type을 모두 Text로 하였다
	 * --> 추후에 클래스 내에서 String으로 변환 후 String --> Integer, Long 등으로 변환
	 */
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		Log log = LogFactory.getLog(MyMapper.class);
		private Text station = new Text();//정류장을 저장할 Text station
		private Text one = new Text("1");//1이라는 값을 저장할 Text one
		/*
		 * 		  번호, 전화번호,     시간, 정류장
		 * input : 1 01012340000 15:36 A         의 형태를 띔
		 * input을 잘라 시간이 0~6시라면 해당 정류장을 count(collect)
		 */
		@Override
		public void map(LongWritable line, Text inputText, OutputCollector<Text, Text> collector, Reporter arg3)
				throws IOException {
			String inputString = inputText.toString();//받은 텍스트를 String형으로 바꾼다
			String[] inputs = inputString.split("[\\W+]");//1 01012340000 15:36 A를 split하여 String배열에 저장
			int hour = 100000, min = 1000000;				// --> [0]에는 번호, [1]에는 전화번호 [2]에는 시간 [3]에는 분 [4]에는 정류장이 할당됨
			try{
				hour = Integer.parseInt(inputs[2]);
				min = Integer.parseInt(inputs[3]);
			}catch(Exception e){
				log.error("<<<<<<<<<<<<<<<<<<<<<<Split err>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				for(int i = 0 ; i < inputs.length; i++) //split이 잘못되었을 때 split된 배열 출력
					log.info(i + " : " + inputs[i]);
				log.info("<<<<<<<<<<<<<<<<<<<<<<Error end>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				return;}//만약 split이 잘못되었다면 다른 형태의 input이 들어온 것이므로 return
			if((hour < 6) || ((hour == 6)&&(min == 0))){//0~5:xx분이거나 6:00인 것만 collect
				String stationInString = inputs[4];		//정류장을 저장
				station.set(stationInString);			//정류장을 Text형 변수에 할당한다
				collector.collect(station,one);			//(정류장,1)을 collect하여 해당 정류장의 통화빈도수를 추가한다
				log.info("MyMapper : (" + station.toString() + ", " + "1)collected");//collect된 값 log로 출력
			}
		}
	}

	public static class Combiner extends MapReduceBase implements Reducer<Text ,Text , Text, Text>{
		Log log = LogFactory.getLog(Combiner.class);
		/*
		 * ---Combiner Class---
		 * Mapper에서 (정류장, 1) 형태로 내보낸 output을 input으로 받아
		 * List 형태로 들어온 1들을 모두 더해 정류장의 통화 빈도수를 측정
		 * output : (빈도 합, 정류장)
		 */
		@Override
		public void reduce(Text inputStation, Iterator<Text> ones, OutputCollector<Text, Text> collector,
				Reporter arg3) throws IOException {
			long sum = 0;
			while(ones.hasNext()){//리스트에 아이템이 더 있다면 --> 합산할 빈도 수가 더 남았다는 것
				ones.next();
				sum += 1;		//합계 값에 1 추가
			}
			Text sumInText = new Text(String.valueOf(sum)); //long 타입을 String으로 변환 --> Text형에 할당
			collector.collect(sumInText, inputStation);	//(빈도 합, 정류장) collect
			log.info("Combine : (" + sumInText.toString() + ", " + inputStation.toString() +")collected");//Collect된 값 로그 출력
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		String four_Stations = "";	//정렬 후 상위 4개 정류장을 나타낼 String ex)"ABCD"
		int[] distanceArray = {700,1500,800,1200,900,800,1300};//각 정류장 사이의 거리를 저장할 배열
		HashMap<String, Long> map = new HashMap<String, Long>();//정렬을 하기 위해 HashMap을 사용 (정류장, 빈도수)
		Log log = LogFactory.getLog(Reduce.class);
		OutputCollector<Text,Text> closingCollector = null; //최종 결과값은 딱 한 번 마지막에 Collect 되어야 한다. close()메소드에서 사용할 outputCollector --> reduce()에서 저장
		/*
		 * 두 정류장 사이의 거리를 구하는 메소드
		 * 상위 4개 정류장을 저장하는 four_Stations의 charAt(index)를 이용하여 input을 준다
		 * char start : 시작 정류장 char end : 도착 정류장
		 */
		private String sumDistance(char start, char end){
			int distance = 0;			//거리를 저장할 변수(return value)
			int startIndex, endIndex;	//distanceArray의 index를 저장할 변수들
			startIndex = (int)start - 65;	//char을 ascii code상에서 (int)A - 65 = 0이다.
			endIndex = (int)end - 65;		//이를 이용해 start, end index를 설정한다.
			if(startIndex < endIndex){
				for(int i = startIndex; i < endIndex; i++)	//시작점이 끝점의 인덱스보다 작으면 인덱스를 더해감
					distance += distanceArray[i];	//시작점과 끝점이 주어지면 그 사이의 값들을 모두 더한다
			}
			else{
				for(int i = startIndex - 1; i >= endIndex; i--)//시작점이 끝점의 인덱스보다 크면 인덱스를 빼감
					distance += distanceArray[i];
			}
			return String.valueOf(distance);	//거리 return;
		}
		/*
		 * 내림차순 정렬 메소드
		 */
		public static <K, V extends Comparable<V>> Map<K, V> 
		sortByValues(final Map<K, V> map) {
			Comparator<K> valueComparator = 
					new Comparator<K>() {
				public int compare(K k1, K k2) {
					int compare = 0;
					if(map.get(k1).compareTo(map.get(k2)) == -1)compare = 1;//compareTo는 오름차순이기 때문에 값을 반대로 설정해줌
					else if(map.get(k1).compareTo(map.get(k2)) == 1)compare = -1;
					if (compare == 0)return 1;
					else return compare;
				}
			};
			Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
			sortedByValues.putAll(map); //위 Comparator를 사용하여 정렬될 Tree map에 input map을 넣음
			return sortedByValues;	//정렬된 TreeMap을 return
		}

		/*
		 * Combine 클래스에서 collect된 output은 모두 merge가 되지 않을 수 있다
		 * ex)두 개의 Mapper가 나누어서 작업을 실행할 경우
		 */
		@Override
		public void reduce(Text frequency, Iterator<Text> stations, OutputCollector<Text, Text> collector, Reporter r)
				throws IOException {
			closingCollector = collector; //close() 메소드에는 Context형 인자가 없기 때문에 결과를 내보내기 위해서 reduce() 메소드의 OutputCollector를 저장
			String station;	//정류장을 저장할 String 변수
			Long tmp = Long.valueOf(0);//빈도수를 저장할 Long 변수
			while(stations.hasNext()){	//빈도 수가 같아서 중복된다면  Iterator를 돌며 정류장들을 Map에 넣어주어야 함
				station = stations.next().toString();	//정류장 String 변수에 저장
				tmp = Long.parseLong(frequency.toString());	//Text로 들어온 frequency를 String으로 변환 후 Long.parseLong
				if(map.get(station) != null){	//station이 null이 아니라면 이미 map에 해당 정류장이 들어가 있다. 이대로 put하게 되면 나중에 넣은 값이 덮어씌워진다.
					Long oldValue = map.get(station);	//따라서 null이 아닐 때는 기존 value를 저장하고
					map.remove(station);				//해당 키 값의 노드를 지운 후
					map.put(station, oldValue + tmp);	//oldvalue와 newvalue를 더하여 새로운 노드로 추가해준다.
				}
				else
					map.put(station, tmp);				//만약 키값이 null이라면 map안에 해당 정류장의 key값이 없는 것이므로 그냥 put하면 된다.
				log.info("Reduce : stations-" + station + "   Value-" + map.get(station));	//put된 key, value Log로 출력
			}
		}

		/*
		 * close() --> Reduce가 끝나고 후처리 작업을 하는 곳(마지막 Reduce 작업)
		 * key = 정류장 순서는 XXXX이며, value = 각 정류장 사이의 거리는 XXXXXX,XXX,XXXXm이다.
		 * 이 output은 reduce가 끝난 후 한 번만 collect되어야 하므로 reduce메소드에 넣어주면 안된다. --> 여러번 collect됨
		 */
		@Override
		public void close(){
			String[] result_Array = new String[4]; //상위 4개 정류장을 정렬할 배열
			Map<String, Long> sortedMap = sortByValues(map);	//Hashmap을 sort하기 위해 sortByValues 사용(내림차순)
			Iterator iterator = sortedMap.entrySet().iterator();	//정렬된 Map의 반복자를 얻어옴(제대로 된 값이 들어왔는지 Log로 확인하기 위해)
			Map.Entry<String, Long> entry;
			while(iterator.hasNext()){
				entry = (Map.Entry<String, Long>)iterator.next();
				log.info("Reduce : Key-" + entry.getKey() + "   Value-" + entry.getValue());
			}
			iterator = sortedMap.keySet().iterator();	//상위 4개의 정류장을 얻어오기 위해 Key(정류장)들의 반복자를 얻어옴
			for(int i =0 ; i< 4; i++)					//상위 4개기 때문에 4번 반복
				if(iterator.hasNext())
					result_Array[i] = (String)iterator.next();
			Arrays.sort(result_Array);	//얻어온 배열 정렬
			for(int i = 0 ; i < 4; i++)
				four_Stations += result_Array[i];	//String형태로 만들어줌 ex)ABCD
			String d1 = sumDistance(four_Stations.charAt(0), four_Stations.charAt(1));	//첫 번째 정류장과 두 번째 정류장 사이의 거리
			String d2 = sumDistance(four_Stations.charAt(1), four_Stations.charAt(2));	//두 번째 정류장과 세 번째 정류장 사이의 거리
			String d3 = sumDistance(four_Stations.charAt(2), four_Stations.charAt(3));	//세 번째 정류장과 네 번째 정류장 사이의 거리
			String key = "정류장 순서는 " + four_Stations + "이며 ";	//output file에 들어갈 key값 세팅
			String valueToString = "각 정류장 간의 거리는 " + d1 + ", " + d2 + ", " + d3 + "m";	//output file에 들어갈 value값 세팅
			try {
				closingCollector.collect(new Text(key),new Text(valueToString));	//Setting한 값들을 Collect
			} catch (IOException e) {
				log.info("[----------------Collect ERROR----------------]\n" +e.getMessage());
			}
			log.info(key + " " + valueToString);	//Collect된 값 로그로 출력
		}
	}
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(Late_Night_Bus.class);
		conf.setJobName("LNB");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(Combiner.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);
	}
}
