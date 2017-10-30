package mimir.sql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QOptimizer implements Statement{
	private SelectBody selectBody;
	private String compileMode;
	private double dataSize;
	private double ucPrct;
	private double timeTB;
	private double timeNaive;
	private HashMap<String,HashSet<String>> uncertAtt;
	private HashMap<String,HashMap<String,HashMap<Integer,HashMap<Double,ArrayList<Double>>>>> timings;
	
	public SelectBody getSelectBody() {
		return selectBody;
	}

	public QOptimizer(SelectBody selectBody, double dataSize, double ucPrct) {
		this.selectBody = selectBody;
		this.dataSize = dataSize;
		this.timeNaive = 0;
		this.timeTB = 0;
		timings = new HashMap<String,HashMap<String,HashMap<Integer,HashMap<Double,ArrayList<Double>>>>>();
		uncertAtt = new HashMap<String,HashSet<String>>();
		calcClosestUncerPrect(ucPrct);
		getAnalysisTimings();
		getUncertainAttributes();
		generateCompileMode();
	}

	private void getAnalysisTimings() {
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
	}

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

	private void generateCompileMode() {
		
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
					if(uncertAtt.get(((Column)temp.getLeftExpression()).getTable().getName()).contains(((Column)temp.getLeftExpression()).getColumnName()))
						joinCnt++;
					if(uncertAtt.get(((Column)temp.getRightExpression()).getTable().getName()).contains(((Column)temp.getRightExpression()).getColumnName()))
						joinCnt++;
					computeTime(joinCnt,"join");
				}
				else if(temp.getLeftExpression() instanceof Column)
				{
					if(uncertAtt.get(((Column)temp.getLeftExpression()).getTable().getName()).contains(((Column)temp.getLeftExpression()).getColumnName()))
						selectionCnt++;
				}
				else if(temp.getRightExpression() instanceof Column)
				{
					if(uncertAtt.get(((Column)temp.getRightExpression()).getTable().getName()).contains(((Column)temp.getRightExpression()).getColumnName()))
						selectionCnt++;
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
				if(uncertAtt.get(att.getTable().getName()).contains(att.getColumnName()))
					grpbyCnt++;
			}
			computeTime(grpbyCnt,"groupby");
		}
		
		List<OrderByElement> ordBy = select.getOrderByElements();
		if(ordBy!=null)
		{
			int ordbyCnt=0;
			for(OrderByElement att:ordBy)
			{
				if(uncertAtt.get(((Column)att.getExpression()).getTable().getName()).contains(((Column)att.getExpression()).getColumnName()))
					ordbyCnt++;
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
	}

	public double getTimeTB() {
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
	}

	public void setSelectBody(SelectBody selectBody) {
		this.selectBody = selectBody;
	}

	public String getCompileMode() {
		return compileMode;
	}

	public void setCompileMode(String compileMode) {
		this.compileMode = compileMode;
	}

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
