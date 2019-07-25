package org.compiere.model;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;

import org.adempiere.exceptions.DBException;
import org.compiere.model.X_T_CostVariance;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;

public class MCostVariance extends X_T_CostVariance{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6646559328650357847L;
	private static final int T_CostVariance_ID = 0;
	private static final int M_Product_ID = 0;
	/**	Static Logger	*/
	private static CLogger	s_log	= CLogger.getCLogger (MCostVariance.class);
	
	/**
	 *  Create & Load existing Persistent Object
	 *  @param ctx context
	 *  @param XX_Material_ID  The unique ID of the object
	 *  @param trxName transaction name
	 */
	public MCostVariance(Properties ctx, int T_CostVariance_ID, String trxName) {
		super(ctx, T_CostVariance_ID, trxName);
		
	}

	/**
	 *  Create & Load existing Persistent Object.
	 *  @param ctx context
	 *  @param rs optional - load from current result set position (no navigation, not closed)
	 *  	if null, a new record is created.
	 *  @param trxName transaction name
	 */
	public MCostVariance(Properties ctx, ResultSet rs, String trxName) {
		super(ctx, rs, trxName);
		// other initializations here
	}
	
	
	public static MCostVariance get(Properties ctx, int T_CostVariance_ID, int M_Product_ID, String trxName) {
		MCostVariance retValue = null;
	    String sql = "SELECT * FROM T_CostVariance WHERE T_CostVariance_ID=? AND M_Product_ID=?";
	    PreparedStatement pstmt = null;
	       try {
	            pstmt = DB.prepareStatement (sql, trxName);
	            pstmt.setInt(1, T_CostVariance_ID);
	            pstmt.setInt(2, M_Product_ID);
	            final ResultSet rs = pstmt.executeQuery ();
	            if (rs.next ()){
	               retValue = new MCostVariance(ctx, rs, trxName);
	            }
	            rs.close ();
	            pstmt.close ();
	            pstmt = null;
	       } catch (SQLException e){
	            s_log.log(Level.SEVERE, sql, e);
	       } 
	       try {
	            if (pstmt != null){
	               pstmt.close ();
	            }
	            pstmt = null;
	       } catch (SQLException e)	{
	            pstmt = null;
	       }
	       return retValue;
	}
	
	public String Savecostvariance_average(String trx,
			int p_ad_org_id,
			BigDecimal p_currentcostprice,
			BigDecimal p_currentqty,
			int p_m_attributesetinstance_id,
			int p_m_costelement_id,
			int p_m_product_id)
	{
		MCostElement cl=new MCostElement(Env.getCtx(), p_m_costelement_id, trx);
		int p_ad_client_id=cl.getAD_Client_ID();
		int p_c_acctschema_id=c_acctschemat(p_ad_client_id,trx);
		int AD_User_ID = Env.getContextAsInt(Env.getCtx(), "#AD_User_ID");
		
		MAcctSchema as=new MAcctSchema (Env.getCtx(), p_c_acctschema_id, trx);
		String p_costingmethod="";
		BigDecimal p_cumulatedamt=Env.ZERO;
		BigDecimal p_cumulatedqty=Env.ZERO;
		BigDecimal p_currentcostpricell=Env.ZERO;
		if(as.getCostingMethod()!=null){
			p_costingmethod=as.getCostingMethod();
		}
		String retValue="";
	    String sql = "SELECT * FROM costvariance_average ("+
	    				p_ad_client_id+","+
	    				p_ad_org_id+","+
						p_c_acctschema_id+",'"+
						p_costingmethod +"',"+
						AD_User_ID +","+
						p_cumulatedamt +","+
						p_cumulatedqty +","+
						p_currentcostprice +","+
						p_currentcostpricell +","+
						p_currentqty +","+
						p_m_attributesetinstance_id +","+
						p_m_costelement_id +","+
						as.getM_CostType_ID() +","+
						p_m_product_id +","+
						AD_User_ID+" )";
	   PreparedStatement pstmt = null;
       try {
            pstmt = DB.prepareStatement (sql, trx);
            final ResultSet rs = pstmt.executeQuery ();
            if (rs.next ()){
              retValue = rs.getString(1);
            }
            rs.close ();
            pstmt.close ();
            pstmt = null;
       } catch (SQLException e){
            s_log.log(Level.SEVERE, sql, e);
            retValue=e.getMessage();
       } 
       try {
            if (pstmt != null){
               pstmt.close ();
            }
            pstmt = null;
       } catch (SQLException e)	{
            pstmt = null;
       }
       return retValue;
	}
	
	public String costvariance_average_cost_collector(String trx,
			int p_ad_org_id,
			BigDecimal p_currentcostprice,
			BigDecimal p_currentqty,
			int p_m_attributesetinstance_id,
			int p_m_costelement_id,
			int p_m_product_id)
	{
		MCostElement cl=new MCostElement(Env.getCtx(), p_m_costelement_id, trx);
		int p_ad_client_id=cl.getAD_Client_ID();
		int p_c_acctschema_id=c_acctschemat(p_ad_client_id,trx);
		int AD_User_ID = Env.getContextAsInt(Env.getCtx(), "#AD_User_ID");
		
		MAcctSchema as=new MAcctSchema (Env.getCtx(), p_c_acctschema_id, trx);
		String p_costingmethod="";
		BigDecimal p_cumulatedamt=Env.ZERO;
		BigDecimal p_cumulatedqty=Env.ZERO;
		BigDecimal p_currentcostpricell=Env.ZERO;
		if(as.getCostingMethod()!=null){
			p_costingmethod=as.getCostingMethod();
		}
		String retValue="";
	    String sql = "SELECT * FROM costvariance_average_cost_collector ("+
	    				p_ad_client_id+","+
	    				p_ad_org_id+","+
						p_c_acctschema_id+",'"+
						p_costingmethod +"',"+
						AD_User_ID +","+
						p_cumulatedamt +","+
						p_cumulatedqty +","+
						p_currentcostprice +","+
						p_currentcostpricell +","+
						p_currentqty +","+
						p_m_attributesetinstance_id +","+
						p_m_costelement_id +","+
						as.getM_CostType_ID() +","+
						p_m_product_id +","+
						AD_User_ID+" )";
	   PreparedStatement pstmt = null;
       try {
            pstmt = DB.prepareStatement (sql, trx);
            final ResultSet rs = pstmt.executeQuery ();
            if (rs.next ()){
              retValue = rs.getString(1);
            }
            rs.close ();
            pstmt.close ();
            pstmt = null;
       } catch (SQLException e){
            s_log.log(Level.SEVERE, sql, e);
            retValue=e.getMessage();
       } 
       try {
            if (pstmt != null){
               pstmt.close ();
            }
            pstmt = null;
       } catch (SQLException e)	{
            pstmt = null;
       }
       return retValue;
	}
		
	
	public String createCostVariancedetail (int AD_Org_ID, int M_Product_ID, int M_AttributeSetInstance_ID,
			int Line_ID, int M_CostElement_ID, BigDecimal Amt, BigDecimal Qty,  String type, String trxName,
			String IsSotrx)
	{
		String retValue="@OK@";

		int T_CostVariancedetail_ID=0;
	    String sql = "SELECT coalesce(T_CostVariancedetail_ID,0) FROM T_CostVariancedetail"
	    		+ " WHERE AD_Org_ID="+AD_Org_ID+
	    		 " AND M_Product_ID ="+M_Product_ID+
	    		 " AND M_AttributeSetInstance_ID="+M_AttributeSetInstance_ID+
	    		 " AND M_CostElement_ID= "+M_CostElement_ID+
	    		 " AND IsSotrx='"+IsSotrx+
	    		 "' ";
	    if(type.compareTo("M_MovementLine_ID")==0){
	    	sql =sql+ " AND M_MovementLine_ID="+Line_ID;
	    }
	    else if(type.compareTo("M_InventoryLine_ID")==0){
	    	sql =sql+ " AND M_InventoryLine_ID="+Line_ID;
	    }
	    else if(type.compareTo("M_InOutLine_ID")==0){
	    	sql =sql+ " AND M_InOutLine_ID="+Line_ID;
	    }
	    else if(type.compareTo("PP_Cost_Collector_ID")==0){
	    	sql =sql+ " AND PP_Cost_Collector_ID="+Line_ID;
	    }
	    else if(type.compareTo("M_MatchInv_ID")==0){
	    	sql =sql+ " AND M_MatchInv_ID="+Line_ID;
	    }
	    else if(type.compareTo("M_ProductionLine_ID")==0){
	    	sql =sql+ " AND M_ProductionLine_ID="+Line_ID;
	    }
	    
	    
	    PreparedStatement pstmt = null;
	       try {
	            pstmt = DB.prepareStatement (sql, trxName);
	            final ResultSet rs = pstmt.executeQuery ();
	            if (rs.next ()){
	            	T_CostVariancedetail_ID = rs.getInt(1);
	            }
	            rs.close ();
	            pstmt.close ();
	            pstmt = null;
	       } catch (SQLException e){
	            s_log.log(Level.SEVERE, sql, e);
	            try {
		            if (pstmt != null){
		               pstmt.close ();
		            }
		            pstmt = null;
		       } catch (SQLException e1)	{
		            pstmt = null;
		       }
	            return ""+e;
	       } 
	       try {
	            if (pstmt != null){
	               pstmt.close ();
	            }
	            pstmt = null;
	       } catch (SQLException e)	{
	            pstmt = null;
	       }
	       
	       if(T_CostVariancedetail_ID>1){
	    	   return "@OK@";
	       }
		
	    MCostElement cl=new MCostElement(Env.getCtx(), M_CostElement_ID, trxName);
		int p_ad_client_id=cl.getAD_Client_ID();
		int p_c_acctschema_id=c_acctschemat(p_ad_client_id,trxName);
		int AD_User_ID = Env.getContextAsInt(Env.getCtx(), "#AD_User_ID");
	    X_T_CostVariancedetail cvd= new X_T_CostVariancedetail(getCtx(),0, trxName);
	    if(IsSotrx.equals("N")){
	    	cvd.setIsSOTrx(false);
	    }
	    else{
	    	cvd.setIsSOTrx(true);
	    }
	    cvd.setC_AcctSchema_ID(p_c_acctschema_id);
	    cvd.setAD_Org_ID(AD_Org_ID);
	    cvd.setM_Product_ID(M_Product_ID);
	    cvd.setM_AttributeSetInstance_ID(M_AttributeSetInstance_ID);
	    cvd.setM_CostElement_ID(M_CostElement_ID);
	    cvd.setAmt(Amt);
	    cvd.setQty(Qty);
	    if(type.compareTo("M_MovementLine_ID")==0){
	    	cvd.setM_MovementLine_ID(Line_ID);
	    }
	    else if(type.compareTo("M_InventoryLine_ID")==0){
	    	cvd.setM_InventoryLine_ID(Line_ID);
	    }
	    else if(type.compareTo("M_InOutLine_ID")==0){
	    	cvd.setM_InOutLine_ID(Line_ID);
	    }
	    else if(type.compareTo("PP_Cost_Collector_ID")==0){
	    	cvd.setPP_Cost_Collector_ID(Line_ID);
	    }
	    else if(type.compareTo("M_MatchInv_ID")==0){
	    	cvd.setM_MatchInv_ID(Line_ID);
	    }
	    else if(type.compareTo("M_ProductionLine_ID")==0){
	    	cvd.setM_ProductionLine_ID(Line_ID);
	    }
	    if(!cvd.save()){
	    	
	    	return " Data not saved in T_CostVariancedetail Table";
	    }
		return retValue;
	}
	
	
	public ArrayList findVariance(int AD_Org_ID, int M_Product_ID, int M_AttributeSetInstance_ID,String trxName){
		ArrayList VarianceData =new ArrayList();
		StringBuffer sql = new StringBuffer("");
		int AD_Client_ID=Env.getAD_Client_ID(Env.getCtx());
		sql.append( " select T_costVariance_ID,M_CostElement_ID,currentqty,currentcostprice from T_costVariance"
				+ " where isactive='Y' AND ad_org_ID="+AD_Org_ID
				+ " and M_product_ID="+M_Product_ID
				+ " and m_attributesetinstance_ID="+M_AttributeSetInstance_ID
				+ "");

		//System.out.println("getProductinfo sql.toString()$$$$$$$$$$$$::"+sql.toString());
		Boolean conf=false;
			try
			{
				PreparedStatement pstmt = DB.prepareStatement(sql.toString(), trxName);
				ResultSet rs = pstmt.executeQuery();
				int i=1;
				while (rs.next())
				{
					if(rs.getBigDecimal(3).signum()==0){ //rifat added 25 april 2019. Not need zero qty
						continue;
					}
					
					Vector<Object> line = new Vector<Object>();	
					line.add(rs.getInt(1));
					line.add(rs.getInt(2));
					line.add(rs.getBigDecimal(3));
					line.add(rs.getBigDecimal(4));
					VarianceData.add(line);
				}
			rs.close();
			pstmt.close();
			
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		return VarianceData;
	}
	
	public ArrayList findVarianceForMMReturns(int AD_Org_ID, int M_Product_ID, int M_AttributeSetInstance_ID,String trxName){
		ArrayList VarianceData =new ArrayList();
		StringBuffer sql = new StringBuffer("");
		int AD_Client_ID=Env.getAD_Client_ID(Env.getCtx());
		sql.append( " select T_costVariance_ID,M_CostElement_ID,currentqty,currentcostprice from T_costVariance"
				+ " where isactive='Y' AND ad_org_ID="+AD_Org_ID
				+ " and M_product_ID="+M_Product_ID
				+ " and m_attributesetinstance_ID="+M_AttributeSetInstance_ID
				+ "");

		//System.out.println("getProductinfo sql.toString()$$$$$$$$$$$$::"+sql.toString());
		Boolean conf=false;
			try
			{
				PreparedStatement pstmt = DB.prepareStatement(sql.toString(), trxName);
				ResultSet rs = pstmt.executeQuery();
				int i=1;
				while (rs.next())
				{
					
					
					Vector<Object> line = new Vector<Object>();	
					line.add(rs.getInt(1));
					line.add(rs.getInt(2));
					line.add(rs.getBigDecimal(3));
					line.add(rs.getBigDecimal(4));
					VarianceData.add(line);
				}
			rs.close();
			pstmt.close();
			
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		return VarianceData;
	}
	
	
	
	public ArrayList findVarianceDetails(int AD_Org_ID, int M_Product_ID, int M_AttributeSetInstance_ID,
			int Line_ID,String type,String trxName){
		ArrayList VarianceData =new ArrayList();
		StringBuffer sql = new StringBuffer("");
		int AD_Client_ID=Env.getAD_Client_ID(Env.getCtx());
		sql.append( " Select T_costVariancedetail_ID,M_CostElement_ID,qty,round(amt,15) from T_costVariancedetail"
				+ " where isactive='Y' AND ad_org_ID="+AD_Org_ID
				+ " and M_product_ID="+M_Product_ID
				+ " and m_attributesetinstance_ID="+M_AttributeSetInstance_ID
				+ "");

		if(type.compareTo("M_MovementLine_ID")==0){
			sql.append(" AND M_MovementLine_ID="+Line_ID);
	    }
	    else if(type.compareTo("M_InventoryLine_ID")==0){
	    	sql.append(" AND M_InventoryLine_ID="+Line_ID);
	    }
	    else if(type.compareTo("M_InOutLine_ID")==0){
	    	sql.append(" AND M_InOutLine_ID="+Line_ID);
	    }
	    else if(type.compareTo("PP_Cost_Collector_ID")==0){
	    	sql.append(" AND PP_Cost_Collector_ID="+Line_ID);
	    }
	    else if(type.compareTo("M_MatchInv_ID")==0){
	    	sql.append(" AND M_MatchInv_ID="+Line_ID);
	    }
	    else if(type.compareTo("M_ProductionLine_ID")==0){
	    	sql.append(" AND M_ProductionLine_ID="+Line_ID);
	    }
		
		
		//System.out.println("getProductinfo sql.toString()$$$$$$$$$$$$::"+sql.toString());
		Boolean conf=false;
			try
			{
				PreparedStatement pstmt = DB.prepareStatement(sql.toString(), trxName);
				ResultSet rs = pstmt.executeQuery();
				int i=1;
				while (rs.next())
				{
					Vector<Object> line = new Vector<Object>();	
					line.add(rs.getInt(1));
					line.add(rs.getInt(2));
					line.add(rs.getBigDecimal(3));
					line.add(rs.getBigDecimal(4));
					VarianceData.add(line);
				}
			rs.close();
			pstmt.close();
			
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		return VarianceData;
	}
	
	public int c_acctschemat(int ad_client_id, String trx) {
		int p_c_acctschema_id=0;
	    String sql = "SELECT c_acctschema_id FROM c_acctschema WHERE ad_client_id="+ad_client_id;
	    PreparedStatement pstmt = null;
	       try {
	            pstmt = DB.prepareStatement (sql, trx);
	            final ResultSet rs = pstmt.executeQuery ();
	            if (rs.next ()){
	            	p_c_acctschema_id = rs.getInt(1);
	            }
	            rs.close ();
	            pstmt.close ();
	            pstmt = null;
	       } catch (SQLException e){
	            s_log.log(Level.SEVERE, sql, e);
	       } 
	       try {
	            if (pstmt != null){
	               pstmt.close ();
	            }
	            pstmt = null;
	       } catch (SQLException e)	{
	            pstmt = null;
	       }
	       return p_c_acctschema_id;
	}
	public static int getCost_Element_Variance (int costElementData_ID,MAcctSchema as, String trxName )
	{
		int M_CostElementvariance_ID=0;
		String sql = " SELECT M_CostElementvariance_ID FROM  M_CostElement_Acct "+
				" WHERE  m_costElement_ID=? AND C_AcctSchema_ID=? AND AD_Client_ID=? and isActive='Y'";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql, trxName);
			pstmt.setInt (1, costElementData_ID);
			pstmt.setInt (2, as.getC_AcctSchema_ID());
			pstmt.setInt (3, as.getAD_Client_ID());
			rs = pstmt.executeQuery ();
			while (rs.next ())
			{
				M_CostElementvariance_ID=rs.getInt(1);
			}
		}
		catch (SQLException e)
		{
			throw new DBException(e, sql);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}

		
		return M_CostElementvariance_ID;
	}
	
	public int Find_costvariance_average(String trx,
			int p_ad_org_id,
			int p_m_attributesetinstance_id,
			int p_m_costelement_id,
			int p_m_product_id) 
	{
		int T_CostVariancedetail_ID=-1;
	    String sql = " SELECT T_CostVariancedetail_ID FROM T_CostVariancedetail cc"
	    		+ " where  ad_org_Id="+p_ad_org_id
	    		+ " and m_product_id="+p_m_product_id
	    		+ " and m_attributesetinstance_Id="+p_m_attributesetinstance_id
	    		+ " and m_costelement_id ="+p_m_costelement_id;
	    PreparedStatement pstmt = null;
	       try {
	            pstmt = DB.prepareStatement (sql, trx);
	            final ResultSet rs = pstmt.executeQuery ();
	            if (rs.next ()){
	            	T_CostVariancedetail_ID = rs.getInt(1);
	            }
	            rs.close ();
	            pstmt.close ();
	            pstmt = null;
	       } catch (SQLException e){
	            s_log.log(Level.SEVERE, sql, e);
	       } 
	       try {
	            if (pstmt != null){
	               pstmt.close ();
	            }
	            pstmt = null;
	       } catch (SQLException e)	{
	            pstmt = null;
	       }
	       return T_CostVariancedetail_ID;
	}
}





