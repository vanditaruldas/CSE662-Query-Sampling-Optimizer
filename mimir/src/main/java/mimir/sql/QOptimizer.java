package mimir.sql;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class QOptimizer implements Statement{
	private SelectBody selectBody;
	private String compileMode;
	private double dataSize;
	private double ucPrct;
	
	
	public SelectBody getSelectBody() {
		return selectBody;
	}

	public QOptimizer(SelectBody selectBody, double dataSize, double ucPrct) {
		this.selectBody = selectBody;
		this.dataSize = dataSize;
		this.ucPrct = ucPrct;
		generateCompileMode();
	}

	private void generateCompileMode() {
				
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
