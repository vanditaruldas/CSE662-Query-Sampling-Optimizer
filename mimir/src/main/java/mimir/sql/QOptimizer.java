package mimir.sql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QOptimizer implements Statement{
	private SelectBody selectBody;
	//private String compileMode;
	private double dataSize;
	private double ucPrct;
	//private double timeTB;
	//private double timeNaive;
	private String query;
	private HashMap<String,HashSet<String>> uncertAtt;
	//private HashMap<String,HashMap<String,HashMap<Integer,HashMap<Double,ArrayList<Double>>>>> timings;
	private HashMap<String,HashMap<String,HashMap<Integer,Double>>> timings;
	private boolean singleTable;
	private String singleTableName;
	private HashMap<String,String> aliasMap = new HashMap<String,String>();
	private boolean IL;
	private HashSet<String> uncertSet;
	
	public SelectBody getSelectBody() {
		return selectBody;
	}

	public QOptimizer(SelectBody selectBody, double dataSize, double ucPrct) {
		StringReader input = new StringReader(selectBody.toString().toUpperCase());
		CCJSqlParser parser = new CCJSqlParser(input);
		Statement query = null;
		try
		{
			query = parser.Statement();
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		this.selectBody = ((Select)query).getSelectBody();
		this.setQuery(this.getSelectBody().toString());
		//this.selectBody = selectBody;
		//this.setQuery(selectBody.toString().toUpperCase());
		this.dataSize = dataSize;
		//this.timeNaive = 0;
		//this.timeTB = 0;
		this.singleTable = false;
		this.IL=false;
		//this.timings = new HashMap<String,HashMap<String,HashMap<Integer,HashMap<Double,ArrayList<Double>>>>>();
		this.timings = new HashMap<String,HashMap<String,HashMap<Integer,Double>>>();
		this.uncertAtt = new HashMap<String,HashSet<String>>();
		this.uncertSet = new HashSet<String>();
		calcClosestUncerPrect(ucPrct);
		//getAnalysisTimings();
		getUncertainAttributes();
		changeQuery();
		generateAnalysisTimings();
		checkJoin((PlainSelect)this.getSelectBody());
		updateSelectBody();
		generateuncertSet();
		//generateCompileMode();
		
	}
	
	private void generateuncertSet() {
		if(this.isSingleTable())
		{
			if(this.uncertAtt.containsKey(this.getSingleTableName()))
			{
				Iterator<String> temp = this.uncertAtt.get(this.getSingleTableName()).iterator();
				while(temp.hasNext())
				{
					this.uncertSet.add(this.getSingleTableName().concat("_RUN_1_").concat(temp.next()));
				}
			}
		}
		else
		{
			 Iterator<String> temp = aliasMap.keySet().iterator();
			 while(temp.hasNext())
			 {
				 String al = temp.next();
				 HashSet<String> set = this.uncertAtt.get(aliasMap.get(al));
				 if(set!=null)
				 {
					 Iterator<String> iter = set.iterator();
					 while(iter.hasNext())
					 {
						 this.uncertSet.add(al.concat("_").concat(iter.next()));
					 }
				 }
			 }
			 
		}
	}

	private void updateSelectBody()
	{
		StringReader input = new StringReader(this.getQuery());
		CCJSqlParser parser = new CCJSqlParser(input);
		Statement query = null;
		try
		{
			query = parser.Statement();
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		this.setSelectBody(((Select)query).getSelectBody());
	}

	private void changeQuery() {
		String name = ((Table)((PlainSelect)this.getSelectBody()).getFromItem()).getName();
		if(uncertAtt.containsKey(name))
			this.setQuery(this.getQuery().replaceAll(name.concat(" "), name.concat("_RUN_1 ")));
		
		if(((Table)((PlainSelect)this.getSelectBody()).getFromItem()).getAlias() != null)
			aliasMap.put(((Table)((PlainSelect)this.getSelectBody()).getFromItem()).getAlias(), name);
		List<Join> tables = ((PlainSelect)this.getSelectBody()).getJoins();
		if(tables != null)
		{
			for(Join j:tables)
			{
				name = ((Table)j.getRightItem()).getName().toUpperCase();
				if(uncertAtt.containsKey(name))
					this.setQuery(this.getQuery().replaceAll(name.concat(" "), name.concat("_RUN_1 ")));
				
				if(((Table)j.getRightItem()).getAlias() != null)
					aliasMap.put(((Table)j.getRightItem()).getAlias(), name);
			}
		}
		else
		{
			this.setSingleTable(true);
			this.setSingleTableName(name);
		}
	}
	
	private void generateAnalysisTimings() {
		BufferedReader reader;
		try 
		{
			reader = new BufferedReader(new FileReader("test/UncertaintyList/timings.txt"));
			String line;
			while ((line = reader.readLine()) != null)
			{
				String[] split = line.split(" ");
				if(Double.parseDouble(split[3]) == this.getUcPrct())
				{
					ArrayList<Double> attList = new ArrayList<Double>();
					for(int i=4;i<split.length;i++)
						attList.add(Double.parseDouble(split[i]));
	
					if(!timings.containsKey(split[0]))
					{
						timings.put(split[0], new HashMap<String,HashMap<Integer,Double>>());
					}
					if(!timings.get(split[0]).containsKey(split[1]))
					{
						timings.get(split[0]).put(split[1], new HashMap<Integer,Double>());
					}
					double y = (new BigDecimal((attList.get(1)-attList.get(0))/(attList.get(3)-attList.get(2))).multiply(new BigDecimal(this.getDataSize()-attList.get(2)))).add(new BigDecimal(attList.get(0))).doubleValue();		
					timings.get(split[0]).get(split[1]).put(Integer.parseInt(split[2]), y);
				}
			}
			reader.close();	
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	/*private void getAnalysisTimings() {
		BufferedReader reader;
		try 
		{
			reader = new BufferedReader(new FileReader("test/UncertaintyList/timings.txt"));
			String line;
			while ((line = reader.readLine()) != null)
			{
				String[] split = line.split(" ");
				ArrayList<Double> attList = new ArrayList<Double>();
				for(int i=4;i<split.length;i++)
					attList.add(Double.parseDouble(split[i]));

				if(!timings.containsKey(split[0]))
				{
					timings.put(split[0], new HashMap<String,HashMap<Integer,HashMap<Double,ArrayList<Double>>>>());
				}
				if(!timings.get(split[0]).containsKey(split[1]))
				{
					timings.get(split[0]).put(split[1], new HashMap<Integer,HashMap<Double,ArrayList<Double>>>());
				}
				if(!timings.get(split[0]).get(split[1]).containsKey(Integer.parseInt(split[2])))
				{
					timings.get(split[0]).get(split[1]).put(Integer.parseInt(split[2]), new HashMap<Double,ArrayList<Double>>());
				}
				timings.get(split[0]).get(split[1]).get(Integer.parseInt(split[2])).put(Double.parseDouble(split[3]), attList);
			}
			reader.close();	
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}	
	}*/

	private void calcClosestUncerPrect(double ucPrct2) {
		Double[] list= {1.00,5.00,10.00};
		double min=Double.MAX_VALUE,temp;
		for(Double prct:list)
		{
			temp = Math.abs(prct-ucPrct2);
			if(temp<min)
			{
				min = temp;
				this.setUcPrct(prct);
			}
		}
	}

	private void getUncertainAttributes() {
		try 
		{
			BufferedReader reader = new BufferedReader(new FileReader("test/UncertaintyList/UncertaintyList.txt"));
			String line;
			while ((line = reader.readLine()) != null)
			{
				String[] split = line.split(" ");
				HashSet<String> attList = new HashSet<String>();
				for(int i=1;i<split.length;i++)
					attList.add(split[i]);
				uncertAtt.put(split[0], attList);
			}
			reader.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	private void checkJoin(PlainSelect select)
	{
		if(!(select.getFromItem() instanceof Table))
			checkJoin((PlainSelect)((SubSelect)select.getFromItem()).getSelectBody());
		
		BinaryExpression temp;
		if(select.getWhere() != null)
		{
			BinaryExpression e = (BinaryExpression)select.getWhere();
			boolean cont= true;
			while(cont)
			{
				if(e instanceof AndExpression)
				{
					temp = (BinaryExpression) e.getRightExpression();
					e = (BinaryExpression) e.getLeftExpression();
				}
				else
				{
					temp = e;
					cont = false;
				}
				
				if((temp.getLeftExpression() instanceof Column)&&(temp.getRightExpression() instanceof Column))
				{
					String tablName = aliasMap.get(((Column)temp.getLeftExpression()).getTable().getName());
					if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getLeftExpression()).getColumnName()))
						this.setIL(true);
					tablName = aliasMap.get(((Column)temp.getRightExpression()).getTable().getName());
					if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getRightExpression()).getColumnName()))
						this.setIL(true);
				}
				else if(temp.getLeftExpression() instanceof Column)
				{
					if(this.isSingleTable())
					{
						if(uncertAtt.containsKey(this.getSingleTableName()) && uncertAtt.get(this.getSingleTableName()).contains(((Column)temp.getLeftExpression()).getColumnName()))
							this.setIL(true);
					}
					else
					{
						String tablName = aliasMap.get(((Column)temp.getLeftExpression()).getTable().getName());
						if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getLeftExpression()).getColumnName()))
							this.setIL(true);					
					}

				}
				else if(temp.getRightExpression() instanceof Column)
				{
					if(this.isSingleTable())
					{
						if(uncertAtt.containsKey(this.getSingleTableName()) && uncertAtt.get(this.getSingleTableName()).contains(((Column)temp.getRightExpression()).getColumnName()))
							this.setIL(true);
					}
					else
					{
						String tablName = aliasMap.get(((Column)temp.getRightExpression()).getTable().getName());
						if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getRightExpression()).getColumnName()))
							this.setIL(true);
					}
				}
			}
		}
	}

	/*private void generateCompileMode() {
		
		evalModeTimings((PlainSelect)this.getSelectBody());
		if((getTimeTB() == getTimeNaive())&&(getTimeTB() == 0))
			this.setCompileMode("TB");
		else if(Math.min(getTimeTB(), getTimeNaive())==getTimeNaive())
			this.setCompileMode("Naive");
		else
			this.setCompileMode("TB");
	}

	private void evalModeTimings(PlainSelect select) {
		if(!(select.getFromItem() instanceof Table))
			evalModeTimings((PlainSelect)((SubSelect)select.getFromItem()).getSelectBody());
		
		BinaryExpression temp;
		if(select.getWhere() != null)
		{
			BinaryExpression e = (BinaryExpression)select.getWhere();
			boolean cont= true;
			int selectionCnt = 0;
			while(cont)
			{
				if(e instanceof AndExpression)
				{
					temp = (BinaryExpression) e.getRightExpression();
					e = (BinaryExpression) e.getLeftExpression();
				}
				else
				{
					temp = e;
					cont = false;
				}
				
				if((temp.getLeftExpression() instanceof Column)&&(temp.getRightExpression() instanceof Column))
				{
					int joinCnt = 0;
					String tablName = aliasMap.get(((Column)temp.getLeftExpression()).getTable().getName());
					if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getLeftExpression()).getColumnName()))
						joinCnt++;
					tablName = aliasMap.get(((Column)temp.getRightExpression()).getTable().getName());
					if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getRightExpression()).getColumnName()))
						joinCnt++;
					computeTime(joinCnt,"join");
				}
				else if(temp.getLeftExpression() instanceof Column)
				{
					if(this.isSingleTable())
					{
						if(uncertAtt.containsKey(this.getSingleTableName()) && uncertAtt.get(this.getSingleTableName()).contains(((Column)temp.getLeftExpression()).getColumnName()))
							selectionCnt++;	
					}
					else
					{
						String tablName = aliasMap.get(((Column)temp.getLeftExpression()).getTable().getName());
						if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getLeftExpression()).getColumnName()))
							selectionCnt++;						
					}

				}
				else if(temp.getRightExpression() instanceof Column)
				{
					if(this.isSingleTable())
					{
						if(uncertAtt.containsKey(this.getSingleTableName()) && uncertAtt.get(this.getSingleTableName()).contains(((Column)temp.getRightExpression()).getColumnName()))
							selectionCnt++;	
					}
					else
					{
						String tablName = aliasMap.get(((Column)temp.getRightExpression()).getTable().getName());
						if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)temp.getRightExpression()).getColumnName()))
							selectionCnt++;
					}
				}
			}
			computeTime(selectionCnt,"selection");
		}
		List<Column> grpBy = select.getGroupByColumnReferences();
		if(grpBy != null)
		{
			int grpbyCnt = 0;
			for(Column att : grpBy)
			{
				if(this.isSingleTable())
				{
					if(uncertAtt.containsKey(this.getSingleTableName()) && uncertAtt.get(this.getSingleTableName()).contains(att.getColumnName()))
						grpbyCnt++;	
				}
				else
				{
					String tablName = aliasMap.get(att.getTable().getName());
					if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(att.getColumnName()))
						grpbyCnt++;
				}
			}
			computeTime(grpbyCnt,"groupby");
		}
		
		List<OrderByElement> ordBy = select.getOrderByElements();
		if(ordBy!=null)
		{
			int ordbyCnt=0;
			for(OrderByElement att:ordBy)
			{
				if(this.isSingleTable())
				{
					if(uncertAtt.containsKey(this.getSingleTableName()) && uncertAtt.get(this.getSingleTableName()).contains(((Column)att.getExpression()).getColumnName()))
						ordbyCnt++;	
				}
				else
				{
					String tablName = aliasMap.get(((Column)att.getExpression()).getTable().getName());
					if(uncertAtt.containsKey(tablName) && uncertAtt.get(tablName).contains(((Column)att.getExpression()).getColumnName()))
						ordbyCnt++;
				}
			}
			computeTime(ordbyCnt,"orderby");
		}
	}

	private void computeTime(int cnt,String type)
	{
		if(cnt>2)
			cnt=2;
		
		HashMap<String, HashMap<Integer, HashMap<Double, ArrayList<Double>>>> typeMap = timings.get(type);
		if(typeMap!=null)
		{
			HashMap<Integer, HashMap<Double, ArrayList<Double>>> modeMap = typeMap.get("TB");
			if(modeMap !=null)
			{
				HashMap<Double, ArrayList<Double>> cntMap = modeMap.get(cnt);
				if(cntMap != null)
				{
					ArrayList<Double> uncertList = cntMap.get(this.getUcPrct());
					double y = (new BigDecimal((uncertList.get(1)-uncertList.get(0))/(uncertList.get(3)-uncertList.get(2))).multiply(new BigDecimal(this.getDataSize()-uncertList.get(2)))).add(new BigDecimal(uncertList.get(0))).doubleValue();		
					this.setTimeTB(this.getTimeTB()+y);
				}
				else
				{
					this.setTimeTB(Double.MAX_VALUE);
				}
			}
			else
			{
				this.setTimeTB(Double.MAX_VALUE);
			}
			modeMap = typeMap.get("Naive");
			if(modeMap !=null)
			{
				HashMap<Double, ArrayList<Double>> cntMap = modeMap.get(cnt);
				if(cntMap != null)
				{
					ArrayList<Double> uncertList = cntMap.get(this.getUcPrct());
					double y = (new BigDecimal((uncertList.get(1)-uncertList.get(0))/(uncertList.get(3)-uncertList.get(2))).multiply(new BigDecimal(this.getDataSize()-uncertList.get(2)))).add(new BigDecimal(uncertList.get(0))).doubleValue();		
					this.setTimeNaive(this.getTimeNaive()+y);
				}
				else
				{
					this.setTimeNaive(Double.MAX_VALUE);
				}
			}
			else
			{
				this.setTimeNaive(Double.MAX_VALUE);
			}
		}
		else
		{
			this.setTimeNaive(Double.MAX_VALUE);
			this.setTimeTB(Double.MAX_VALUE);
		}
	}*/

	
	
	public boolean isSingleTable() {
		return singleTable;
	}

	public HashMap<String, String> getAliasMap() {
		return aliasMap;
	}

	public void setAliasMap(HashMap<String, String> aliasMap) {
		this.aliasMap = aliasMap;
	}

	public HashSet<String> getUncertSet() {
		return uncertSet;
	}

	public void setUncertSet(HashSet<String> uncertSet) {
		this.uncertSet = uncertSet;
	}

	public boolean isIL() {
		return IL;
	}

	public void setIL(boolean iL) {
		IL = iL;
	}

	public void setSingleTable(boolean singleTable) {
		this.singleTable = singleTable;
	}

	public String getSingleTableName() {
		return singleTableName;
	}

	public void setSingleTableName(String singleTableName) {
		this.singleTableName = singleTableName;
	}

	public String getQuery() {
		return query;
	}

	public HashMap<String, HashSet<String>> getUncertAtt() {
		return uncertAtt;
	}

	public void setUncertAtt(HashMap<String, HashSet<String>> uncertAtt) {
		this.uncertAtt = uncertAtt;
	}

	public HashMap<String, HashMap<String, HashMap<Integer, Double>>> getTimings() {
		return timings;
	}

	public void setTimings(HashMap<String, HashMap<String, HashMap<Integer, Double>>> timings) {
		this.timings = timings;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	/*public double getTimeTB() {
		return timeTB;
	}

	public void setTimeTB(double timeTB) {
		this.timeTB = timeTB;
	}

	public double getTimeNaive() {
		return timeNaive;
	}

	public void setTimeNaive(double timeNaive) {
		this.timeNaive = timeNaive;
	}*/

	public void setSelectBody(SelectBody selectBody) {
		this.selectBody = selectBody;
	}

	/*public String getCompileMode() {
		return compileMode;
	}

	public void setCompileMode(String compileMode) {
		this.compileMode = compileMode;
	}*/

	public double getDataSize() {
		return dataSize;
	}

	public void setDataSize(double dataSize) {
		this.dataSize = dataSize;
	}

	public double getUcPrct() {
		return ucPrct;
	}

	public void setUcPrct(double ucPrct) {
		this.ucPrct = ucPrct;
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {
		Select sel = new Select();
		sel.setSelectBody(selectBody);
		statementVisitor.visit(sel);
	}
}
