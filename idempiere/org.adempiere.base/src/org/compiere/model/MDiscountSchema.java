/******************************************************************************
 * Product: Adempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 1999-2006 ComPiere, Inc. All Rights Reserved.                *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 * For the text or an alternative of this public license, you may reach us    *
 * ComPiere, Inc., 2620 Augustine Dr. #245, Santa Clara, CA 95054, USA        *
 * or via info@compiere.org or http://www.compiere.org/license.html           *
 *****************************************************************************/
package org.compiere.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;

import org.compiere.util.CCache;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.TimeUtil;

//added by sysnova: START
import java.math.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import org.compiere.util.*;
import java.sql.Timestamp;
//added by sysnova: END

/**
 *	Discount Schema Model
 *	
 *  @author Jorg Janke
 *  @version $Id: MDiscountSchema.java,v 1.3 2006/07/30 00:51:04 jjanke Exp $
 */
public class MDiscountSchema extends X_M_DiscountSchema
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3314884382853756019L;


	/**
	 * 	Get Discount Schema from Cache
	 *	@param ctx context
	 *	@param M_DiscountSchema_ID id
	 *	@return MDiscountSchema
	 */
	public static MDiscountSchema get (Properties ctx, int M_DiscountSchema_ID)
	{
		Integer key = Integer.valueOf(M_DiscountSchema_ID);
		MDiscountSchema retValue = (MDiscountSchema) s_cache.get (key);
		if (retValue != null)
			return retValue;
		retValue = new MDiscountSchema (ctx, M_DiscountSchema_ID, null);
		if (retValue.get_ID () != 0)
			s_cache.put (key, retValue);
		return retValue;
	}	//	get

	/**	Cache						*/
	private static CCache<Integer,MDiscountSchema>	s_cache
		= new CCache<Integer,MDiscountSchema>(Table_Name, 20);

	
	/**************************************************************************
	 * 	Standard Constructor
	 *	@param ctx context
	 *	@param M_DiscountSchema_ID id
	 *	@param trxName transaction
	 */
	public MDiscountSchema (Properties ctx, int M_DiscountSchema_ID, String trxName)
	{
		super (ctx, M_DiscountSchema_ID, trxName);
		if (M_DiscountSchema_ID == 0)
		{
		//	setName();
			setDiscountType (DISCOUNTTYPE_FlatPercent);
			setFlatDiscount(Env.ZERO);
			setIsBPartnerFlatDiscount (false);
			setIsQuantityBased (true);	// Y
			setCumulativeLevel(CUMULATIVELEVEL_Line);
		//	setValidFrom (new Timestamp(System.currentTimeMillis()));
		}	
	}	//	MDiscountSchema

	/**
	 * 	Load Constructor
	 *	@param ctx context
	 *	@param rs result set
	 *	@param trxName transaction
	 */
	public MDiscountSchema (Properties ctx, ResultSet rs, String trxName)
	{
		super(ctx, rs, trxName);
	}	//	MDiscountSchema

	/**	Breaks							*/
	private MDiscountSchemaBreak[]	m_breaks  = null;
	/**	Lines							*/
	private MDiscountSchemaLine[]	m_lines  = null;

	/****sysnova********CNV-0001******Added*********/
	int m_DiscountSchemaBreak_ID=0; //21 Jan 09
	/****sysnova********CNV-0001******End*********/
	/**
	 * 	Get Breaks
	 *	@param reload reload
	 *	@return breaks
	 */
	public MDiscountSchemaBreak[] getBreaks(boolean reload)
	{
		if (m_breaks != null && !reload)
			return m_breaks;
		
		String sql = "SELECT * FROM M_DiscountSchemaBreak WHERE M_DiscountSchema_ID=? ORDER BY SeqNo";
		ArrayList<MDiscountSchemaBreak> list = new ArrayList<MDiscountSchemaBreak>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql, get_TrxName());
			pstmt.setInt (1, getM_DiscountSchema_ID());
			rs = pstmt.executeQuery ();
			while (rs.next ())
				list.add(new MDiscountSchemaBreak(getCtx(), rs, get_TrxName()));
		}
		catch (Exception e)
		{
			log.log(Level.SEVERE, sql, e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}
		m_breaks = new MDiscountSchemaBreak[list.size ()];
		list.toArray (m_breaks);
		return m_breaks;
	}	//	getBreaks
	
	/**
	 * 	Get Lines
	 *	@param reload reload
	 *	@return lines
	 */
	public MDiscountSchemaLine[] getLines(boolean reload)
	{
		if (m_lines != null && !reload) {
			set_TrxName(m_lines, get_TrxName());
			return m_lines;
		}
		
		String sql = "SELECT * FROM M_DiscountSchemaLine WHERE M_DiscountSchema_ID=? ORDER BY SeqNo";
		ArrayList<MDiscountSchemaLine> list = new ArrayList<MDiscountSchemaLine>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql, get_TrxName());
			pstmt.setInt (1, getM_DiscountSchema_ID());
			rs = pstmt.executeQuery ();
			while (rs.next ())
				list.add(new MDiscountSchemaLine(getCtx(), rs, get_TrxName()));
		}
		catch (Exception e)
		{
			log.log(Level.SEVERE, sql, e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}
		m_lines = new MDiscountSchemaLine[list.size ()];
		list.toArray (m_lines);
		return m_lines;
	}	//	getBreaks

	/**
	 * 	Calculate Discounted Price
	 *	@param Qty quantity
	 *	@param Price price
	 *	@param M_Product_ID product
	 *	@param M_Product_Category_ID category
	 *	@param BPartnerFlatDiscount flat discount
	 *	@return discount or zero
	 */
	public BigDecimal calculatePrice (BigDecimal Qty, BigDecimal Price,  
		int M_Product_ID, int M_Product_Category_ID,  
		BigDecimal BPartnerFlatDiscount)
	{
		if (log.isLoggable(Level.FINE)) log.fine("Price=" + Price + ",Qty=" + Qty);
		if (Price == null || Env.ZERO.compareTo(Price) == 0)
			return Price;
		//
		BigDecimal discount = calculateDiscount(Qty, Price, 
			M_Product_ID, M_Product_Category_ID, BPartnerFlatDiscount);
		//	nothing to calculate
		if (discount == null || discount.signum() == 0)
			return Price;
		//
		BigDecimal onehundred = Env.ONEHUNDRED;
		BigDecimal multiplier = (onehundred).subtract(discount);
		
		/****sysnova********CNV-0001******blocked 4.1*********
		multiplier = multiplier.divide(onehundred, 6, RoundingMode.HALF_UP);
		BigDecimal newPrice = Price.multiply(multiplier);
		/****sysnova********CNV-0001******End*********/
		/****sysnova********CNV-0001******Added*********/
		multiplier = multiplier.divide(onehundred, 3, BigDecimal.ROUND_HALF_UP);
		//Muntasim 281208
		BigDecimal newPrice = Price.subtract(discount);
		/****sysnova********CNV-0001******End*********/
		if (log.isLoggable(Level.FINE)) log.fine("=>" + newPrice);
		return newPrice;
	}	//	calculatePrice

	/**
	 * 	Calculate Discount Percentage
	 *	@param Qty quantity
	 *	@param Price price
	 *	@param M_Product_ID product
	 *	@param M_Product_Category_ID category
	 *	@param BPartnerFlatDiscount flat discount
	 *	@return discount or zero
	 */
	public BigDecimal calculateDiscount (BigDecimal Qty, BigDecimal Price,  
		int M_Product_ID, int M_Product_Category_ID,
		BigDecimal BPartnerFlatDiscount)
	{
		if (BPartnerFlatDiscount == null)
			BPartnerFlatDiscount = Env.ZERO;
		
		//
		if (DISCOUNTTYPE_FlatPercent.equals(getDiscountType()))
		{
			if (isBPartnerFlatDiscount())
				return BPartnerFlatDiscount;
			return getFlatDiscount();
		}
		//	Not supported
		else if (DISCOUNTTYPE_Formula.equals(getDiscountType())
			|| DISCOUNTTYPE_Pricelist.equals(getDiscountType()))
		{
			if (log.isLoggable(Level.INFO)) log.info ("Not supported (yet) DiscountType=" + getDiscountType());
			return Env.ZERO;
		}
		
		//	Price Breaks
		getBreaks(false);
		BigDecimal Amt = Price.multiply(Qty);
		if (isQuantityBased()) {
			if (log.isLoggable(Level.FINER)) log.finer("Qty=" + Qty + ",M_Product_ID=" + M_Product_ID + ",M_Product_Category_ID=" + M_Product_Category_ID);
		} else {
			if (log.isLoggable(Level.FINER)) log.finer("Amt=" + Amt + ",M_Product_ID=" + M_Product_ID + ",M_Product_Category_ID=" + M_Product_Category_ID);
		}
		for (int i = 0; i < m_breaks.length; i++)
		{
			MDiscountSchemaBreak br = m_breaks[i];
			if (!br.isActive())
				continue;
			
			if (isQuantityBased())
			{
				if (!br.applies(Qty, M_Product_ID, M_Product_Category_ID))
				{
					if (log.isLoggable(Level.FINER)) log.finer("No: " + br);
					continue;
				}
				if (log.isLoggable(Level.FINER)) log.finer("Yes: " + br);
			}
			else
			{
				if (!br.applies(Amt, M_Product_ID, M_Product_Category_ID))
				{
					if (log.isLoggable(Level.FINER)) log.finer("No: " + br);
					continue;
				}
				if (log.isLoggable(Level.FINER)) log.finer("Yes: " + br);
			}
			
			//	Line applies
			BigDecimal discount = null;
			if (br.isBPartnerFlatDiscount())
				discount = BPartnerFlatDiscount;
			else
				discount = br.getBreakDiscount();
			if (log.isLoggable(Level.FINE)) log.fine("Discount=>" + discount);
			return discount;
		}	//	for all breaks
		
		return Env.ZERO;
	}	//	calculateDiscount
	
	
	/**
	 * 	Before Save
	 *	@param newRecord new
	 *	@return true
	 */
	protected boolean beforeSave (boolean newRecord)
	{
		if (getValidFrom() == null)
			setValidFrom (TimeUtil.getDay(null));

		return true;
	}	//	beforeSave
	
	/**
	 * 	Renumber
	 *	@return lines updated
	 */
	public int reSeq()
	{
		int count = 0;
		//	Lines
		MDiscountSchemaLine[] lines = getLines(true);
		for (int i = 0; i < lines.length; i++)
		{
			int line = (i+1) * 10;
			if (line != lines[i].getSeqNo())
			{
				lines[i].setSeqNo(line);
				if (lines[i].save())
					count++;
			}
		}
		m_lines = null;
		
		//	Breaks
		MDiscountSchemaBreak[] breaks = getBreaks(true);
		for (int i = 0; i < breaks.length; i++)
		{
			int line = (i+1) * 10;
			if (line != breaks[i].getSeqNo())
			{
				breaks[i].setSeqNo(line);
				if (breaks[i].save())
					count++;
			}
		}
		m_breaks = null;
		return count;
	}	//	reSeq
	
	

	/****sysnova********CNV-0001******Added*********/
		///////////////////////////////////////////////////////////
		/**
		 * 	Compute Discount 
		 *	@param Qty quantity
		 *	@param Price price
		 *	@param M_Product_ID product
		 *	@param M_Product_Category_ID category
		 *	@param BPartnerFlatDiscount flat discount
		 *	@return Vector
		 *   [0]=discountedPrice
		 *   [1]=discountedQty
		 *   [2]= discountVal
		 *   [3]=
		 *
		 *@author Muntasim
		 *@date 21,22 june 2008 
		 */
		public Vector computeDiscount (Vector discountVector,BigDecimal Qty, BigDecimal Price, 
			int M_Product_ID, int M_Product_Category_ID,
			BigDecimal BPartnerFlatDiscount,boolean BPartner_IsPercentBased)
		{
			Timestamp now = new Timestamp(System.currentTimeMillis());
			//System.out.println("now@@@@@@@@@@@@@@@@@:::"+now);
			
			System.out.println("Qty::"+Qty+"Price::"+Price+"M_Product_ID::"+M_Product_ID+"BPartnerFlatDiscount::"+BPartnerFlatDiscount+"BPartner_IsPercentBased::"+BPartner_IsPercentBased+"getDiscountType()::"+getDiscountType()+"getM_DiscountSchema_ID()::"+getM_DiscountSchema_ID());
			
			BigDecimal discount = new BigDecimal(0);
			BigDecimal discountedPrice=new BigDecimal(0);
			BigDecimal discountedQty=new BigDecimal(0);
			BigDecimal discountVal=new BigDecimal(0);
			BigDecimal discountQty=new BigDecimal(0);
			
			BigDecimal discountValWithoutPercent=new BigDecimal(0);
			
			BigDecimal breakValue=new BigDecimal(0);
			
			String discountCatagory="";
			String discountedPrice_str="0";
			String discountedQty_str="0";
			String discountVal_str="0";
			String discountQty_str="0";
			
			String discountValWithoutPercent_str="0";
			int discountSchemaBreak_id=0;//14 June 10
			if(discountVector !=null && discountVector.size()>0)
			{
				System.out.println("discountVector^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+discountVector.size());
				discountedPrice_str=(String)discountVector.get(0);
				discountedQty_str=(String)discountVector.get(1);
				discountVal_str=(String)discountVector.get(2);
				discountQty_str=(String)discountVector.get(3);
				discountValWithoutPercent_str=(String)discountVector.get(6);

				discountedPrice=new BigDecimal(discountedPrice_str);
				discountedQty=new BigDecimal(discountedQty_str);
				discountVal=new BigDecimal(discountVal_str);
				discountQty=new BigDecimal(discountQty_str);
				
				discountValWithoutPercent=new BigDecimal(discountValWithoutPercent_str);
			}
			else
			{
				discountVector=new Vector();
				discountedPrice=Price;
				discountedQty=Qty;
			}
			
			int discountCombination=getDiscountCombination();
			BigDecimal onehundred = new BigDecimal(100);
			BigDecimal multiplier = new BigDecimal(0);
			BigDecimal dividant = new BigDecimal(0);
			BigDecimal newPrice = new BigDecimal(0);
			BigDecimal convertedDiscountVal = new BigDecimal(0);
			
			BigDecimal Amt = Price.multiply(Qty);
			BigDecimal discount_qty=new BigDecimal(0);
			
			
			System.out.println("discountCombination~~~~~~~~~~~~~~~~~~^"+discountCombination);
		
			boolean isPercentBased=false;
			boolean IsProductBased=false;
			
			switch (discountCombination)
			{    
	        case 11:
	        	//FD_BP----------------Flat discount   Bussiness partner
	        	
	        	System.out.println("-----b4------FD_BP 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:"+discountVal+"-----discount_qty::"+discount_qty+"");        	
	        	discount=BPartnerFlatDiscount;
	        	
	        	if(BPartner_IsPercentBased)
	        	{
	        		multiplier = (onehundred).subtract(discount);
	        		//Muntasim 11 May
	        		//multiplier = multiplier.divide(onehundred, 6, BigDecimal.ROUND_HALF_UP);
	        		multiplier = multiplier.divide(onehundred, 3, BigDecimal.ROUND_HALF_UP);
	        		Amt = Amt.multiply(multiplier);
	        		//Muntasim 11 May
	        		//newPrice=Amt.divide(Qty, 6, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
	        		newPrice=Amt.divide(Qty, 3, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
	        	}
	        	else
	        	{
	        		//Amt=Amt.subtract(discount);
	        		//Muntasim 11 May
	            	//dividant=discount.divide(Qty, 1000, BigDecimal.ROUND_HALF_UP);
	        		dividant=discount.divide(Qty, 3, BigDecimal.ROUND_HALF_UP);
	            	newPrice=Price.subtract(dividant);
	        		Amt = newPrice.multiply(Qty);
	        	}

	        	discountedPrice=newPrice;
	        	
	        	/***************/
	        	discount=Price.subtract(newPrice);
	    		convertedDiscountVal=getPercentedValue(Price, discount);
	    		discountVal=discountVal.add(convertedDiscountVal);
	        	/***************/
	    		
	    		/******/
	    		discountValWithoutPercent=(Price.multiply(Qty)).subtract(newPrice.multiply(Qty));
	    		/*******/
	        	System.out.println("-----after------FD_BP 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:"+discountVal+"-----discount_qty::"+discount_qty+"");
	        	discountCatagory="V";
	           break;
	        case 01:
	        	//FD_V_P-----------------Flat discount Value Based Percent
	        	
	        	System.out.println("-----b4------FD_V_P 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");        	
	        	discount=getFlatDiscount();
	    		
	    		multiplier = (onehundred).subtract(discount);
	    		//Muntasim 11 May
	    		//multiplier = multiplier.divide(onehundred, 6, BigDecimal.ROUND_HALF_UP);
	    		multiplier = multiplier.divide(onehundred, 3, BigDecimal.ROUND_HALF_UP);
	    		Amt = Amt.multiply(multiplier);
	    		//Muntasim 11 May
	    		//newPrice=Amt.divide(Qty, 6, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
	    		newPrice=Amt.divide(Qty, 3, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse

	    		discountedPrice=newPrice;

	        	/***************/
	        	discount=Price.subtract(newPrice);
	    		convertedDiscountVal=getPercentedValue(Price, discount);
	    		discountVal=discountVal.add(convertedDiscountVal);
	        	/***************/
	    		
	    		
	    		/******/
	    		discountValWithoutPercent=(Price.multiply(Qty)).subtract(newPrice.multiply(Qty));
	    		/*******/

	    		
	    		System.out.println("-----after------FD_V_P 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");    		
	    		discountCatagory="V";
	          break;
	        case 00:
	        	
	        	//FD_V_WP----------------Flat discount Value Based Without Percent
	        	discount=getFlatDiscount();
	        	System.out.println("-----b4------FD_V_WP 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"Amt::"+Amt);
	        	/*
	        	 * if price 25 tk and flat discount 7
	        	 * if ordered Qty=17 
	        	 * then dividand=7/17
	        	 * new price=25-7/17
	        	 * and total amt=(25-7/17)*17
	        	 * 
	        	 */
	    		//System.out.println("-----b4------FD_V_WP 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"Amt::"+Amt);
	        	
	        	//Muntasim 11 May
	    		//dividant=discount.divide(Qty, 20, BigDecimal.ROUND_HALF_UP);
	        	dividant=discount.divide(Qty, 3, BigDecimal.ROUND_HALF_UP);
	        	newPrice=Price.subtract(dividant);
	    		Amt = newPrice.multiply(Qty);
	    		//System.out.println("-----b4------FD_V_WP 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"Amt::"+Amt+"newPrice::"+newPrice);
	        	discountedPrice=newPrice;

	        	/***************/
	        	discount=Price.subtract(newPrice);
	    		convertedDiscountVal=getPercentedValue(Price, discount);
	    		discountVal=discountVal.add(convertedDiscountVal);
	        	/***************/

	    		/******/
	    		discountValWithoutPercent=(Price.multiply(Qty)).subtract(newPrice.multiply(Qty));
	    		/*******/

	    		System.out.println("-----after------FD_V_WP 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");        	
	        	discountCatagory="V";
	    		break;
	          case 100:
	        	//BD_VV  ------Break Discount Value/Value

//	        	Price Breaks
	        	System.out.println("-----b4------BD_VV 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");
	        	getBreaks(false);
				for (int i = 0; i < m_breaks.length; i++)
				{
					MDiscountSchemaBreak br = m_breaks[i];
					if (!br.isActive())
						continue;

					//Criteria apply
					if (!br.applies(Amt, M_Product_ID, M_Product_Category_ID))
					{
						log.finer("No: " + br);
						continue;
					}
					else
					{
						//	Line applies
						//System.out.println("Line applies");
						//System.out.println("br.getBreakValue()"+br.getBreakValue());
						discountSchemaBreak_id=br.getM_DiscountSchemaBreak_ID(); //14 Jun 10
						breakValue=br.getBreakValue();

						if (br.isBPartnerFlatDiscount())
						{
							discount = BPartnerFlatDiscount;
							isPercentBased=BPartner_IsPercentBased;
						}
						else
						{
							discount = br.getBreakDiscount();
							isPercentBased=br.isPercentBased();
							IsProductBased=br.IsProductBased();//sysnova added
						}
						
					}
				}	//	for all breaks
				
				/*
				 * sysnova aded 24-14-2014
				 * for set discount with product base.
				 */
				if(IsProductBased){
					if(breakValue.compareTo(Env.ZERO)==1){
						BigDecimal total=Qty.multiply(Price);
						discountQty=  total.divide(breakValue, 5, BigDecimal.ROUND_HALF_UP) ;
					}
					else{
						discountQty=Env.ZERO;
					}
					discountCatagory="QQ";
					break;
				}
				
				//end sysnova
				
				if(isPercentBased)
				{
//		    		multiplier = (onehundred).subtract(discount);
//	    			multiplier = multiplier.divide(onehundred, 6, BigDecimal.ROUND_HALF_UP);
//	    			Amt = Amt.multiply(multiplier);
					/*
					 * 5 % discount 
					 * so new amt is
					 * (100-5)*Amt/100
					 */
		    		multiplier = (onehundred).subtract(discount);
		    		multiplier =multiplier.multiply(Amt);
		    		//Muntasim 11 May
	    			//Amt= multiplier.divide(onehundred, 6, BigDecimal.ROUND_HALF_UP);
		    		Amt= multiplier.divide(onehundred, 3, BigDecimal.ROUND_HALF_UP);
	    			//System.out.println("Amt"+Amt);
				}
				else
				{
					/*
					 * this code has been blocked on 23 sep
					*/ 
					
//					//my codee 3 aug
//					if(breakValue.compareTo(Env.ZERO)!=0)
//					{				
//						BigDecimal discountMultiplier = new BigDecimal(0);
//						/*
//						 *  (Amt)*discountValue/breakValue
//						 */
//						//System.out.println("discount:b4:"+discount);
//						discountMultiplier =discount.multiply(Amt);
//						//System.out.println("discountMultiplier::"+discountMultiplier);					
//						discount= discountMultiplier .divide(breakValue,0,BigDecimal.ROUND_HALF_DOWN); // (discountedQty)/breakQty
//						//System.out.println("discount:aftr:"+discount);
//					}
//					else
//					{
//						//discount= discount;
//					}
					
					Amt=Amt.subtract(discount);
				}
	    		//Muntasim 11 May
	        	//newPrice=Amt.divide(Qty, 6, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
				newPrice=Amt.divide(Qty, 3, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
	        	discountedPrice=newPrice;
	        	
	        	/***************/
	        	discount=Price.subtract(newPrice);
	    		convertedDiscountVal=getPercentedValue(Price, discount);
	    		discountVal=discountVal.add(convertedDiscountVal);
	        	/***************/
	    		
	    		/******/
	    		discountValWithoutPercent=(Price.multiply(Qty)).subtract(newPrice.multiply(Qty));
	    		/*******/

				System.out.println("-----after------BD_VV 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");			
				discountCatagory="VV";
				break;

	         case 101:
	        	//BD_VQ------Break Discount Value/Quantity 

//	        	Price Breaks
	            //System.out.println("-----------BD_VQ 1-------------");
	            System.out.println("-----b4------BD_VQ 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");
	        	getBreaks(false);
				for (int i = 0; i < m_breaks.length; i++)
				{
					MDiscountSchemaBreak br = m_breaks[i];
					if (!br.isActive())
						continue;

					//Criteria apply
					if (!br.applies(Amt, M_Product_ID, M_Product_Category_ID))
					{
						log.finer("No: " + br);
						continue;
					}
					else
					{
						//	Line applies
						//System.out.println("Line applies");
						//System.out.println("br.getBreakValue()"+br.getBreakValue());
						discountSchemaBreak_id=br.getM_DiscountSchemaBreak_ID(); //14 Jun 10
						breakValue=br.getBreakValue();
						
						if (br.isBPartnerFlatDiscount())
						{
							discount = BPartnerFlatDiscount;
							isPercentBased=BPartner_IsPercentBased;
						}
						else
						{
							discount = br.getBreakDiscount();
							isPercentBased=br.isPercentBased();
							IsProductBased=br.IsProductBased();//sysnova added
						}
						log.finer("Yes: " + br);
					}
				}	//	for all breaks

				/*
				 * sysnova aded 24-14-2014
				 * for set discount with product base.
				 
				if(IsProductBased){
					//IsProductBased=br.IsProductBased();//sysnova added
					discountQty=Env.ZERO;   ;
					discountCatagory="QQ";
					break;
				}
				*/
				//end sysnova
				
				if(isPercentBased)
				{
					/*Example:
					 *   5% discount 
					 *   then calculation is 
					 *   100------5
					 *     1------(5/100)
					 *     x------(5/100)*x
					 *   so discount_qty=(5/100)*x
					 */
		    		multiplier =Qty.multiply(discount);// discount*qty
		    		discount_qty = multiplier.divide(onehundred, 0, BigDecimal.ROUND_HALF_UP);//// discount*qty/100
				}
				else
				{
					/*Example:
					 *   5 discount 
					 *   then calculation is 
					 *   discount_qty=x
					 */
					
//					
//					if(breakValue.compareTo(Env.ZERO)!=0)
//					{				
//						BigDecimal discountMultiplier = new BigDecimal(0);
//						/*
//						 *  (Amt)*discount/breakValue
//						 */
//						//System.out.println("discount_qty:b4:"+discount_qty);
//						discountMultiplier =discount.multiply(Amt);
//						//System.out.println("discountMultiplier::"+discountMultiplier);					
//						//discount_qty= discountMultiplier .divide(breakValue,0,BigDecimal.ROUND_HALF_DOWN); // (discountedQty)/breakQty
//						discount_qty= discountMultiplier .divide(breakValue,0,BigDecimal.ROUND_FLOOR); // (discountedQty)/breakQty
//						//System.out.println("discount_qty:aftr:"+discount_qty);
//					}
//					else
//					{
//						discount_qty= discount;
//					}
					discount_qty= discount;
				}
				
				discountQty=discountQty.add(discount_qty);					
				discountCatagory="VQ";
				
				System.out.println("-----after------BD_VQ 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");
				
	            break;
	        case 110:
	        	//BD_QQ------Break Discount Quantity/Quantity
//	        	Price Breaks
	        	System.out.println("-----b4------BD_QQ 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");
	        	
	        	getBreaks(false);
				for (int i = 0; i < m_breaks.length; i++)
				{
					MDiscountSchemaBreak br = m_breaks[i];
					if (!br.isActive())
						continue;

					//System.out.println("Qty: " + Qty);
					//Criteria apply
					if (!br.applies(Qty, M_Product_ID, M_Product_Category_ID))
					{
						System.out.println("Line NOT applies");
						//System.out.println("No: " + br);
						log.finer("No: " + br);
						continue;
					}
					else
					{
						//	Line applies
						System.out.println("Line applies");
						System.out.println("br.getBreakValue()"+br.getBreakValue());
						discountSchemaBreak_id=br.getM_DiscountSchemaBreak_ID(); //14 Jun 10
						breakValue=br.getBreakValue();
						
						if (br.isBPartnerFlatDiscount())
						{
							discount = BPartnerFlatDiscount;
							isPercentBased=BPartner_IsPercentBased;
						}
						else
						{
							discount = br.getBreakDiscount();
							isPercentBased=br.isPercentBased();
							IsProductBased=br.IsProductBased();//sysnova added
						}
						log.finer("Yes: " + br);
					}
				}	//	for all breaks
				
				/*
				 * sysnova aded 24-14-2014
				 * for set discount with product base.
				 */
				if(IsProductBased){
					if(breakValue.compareTo(Env.ZERO)==1){
						//BigDecimal total=Qty.multiply(Price);
						discountQty=  Qty.divide(breakValue, 5, BigDecimal.ROUND_HALF_UP) ;
						
					}
					else{
						discountQty=Env.ZERO;
					}
					discountCatagory="QQ";
					break;
				}
				
				//end sysnova
				
				if(isPercentBased)
				{
					/*Example:
					 *   5% discount 
					 *   then calculation is 
					 *   100------5
					 *     1------(5/100)
					 *     x------(5/100)*x
					 *   so discount_qty=(5/100)*x
					 */
		    		//multiplier = multiplier.divide(onehundred, 0, BigDecimal.ROUND_HALF_UP);
		    		multiplier =Qty.multiply(discount);// discount*qty
		    		//System.out.println("multiplier:::"+multiplier);
		    		discount_qty = multiplier.divide(onehundred, 0, BigDecimal.ROUND_HALF_UP); // (discount*qty)/100
		    		//System.out.println("discount_qty:::"+discount_qty);
				}
				else
				{
					/*Example:
					 *   5 discount 
					 *   then calculation is 
					 *   discount_qty=x
					 */
					//discountedQty
					
					//System.out.println("breakValue::"+breakValue);
//					if(breakValue.compareTo(Env.ZERO)!=0)
//					{	
//						BigDecimal discountMultiplier = new BigDecimal(0);
//						/*
//						 *  (discountedQty)*discount/breakQty
//						 */
//						//System.out.println("discount_qty:b4:"+discount_qty);
//						discountMultiplier =discount.multiply(discountedQty);
////						//System.out.println("discountMultiplier::"+discountMultiplier);					
//						//discount_qty= discountMultiplier .divide(breakValue,0,BigDecimal.ROUND_HALF_DOWN); // (discountedQty)/breakQty
//						discount_qty= discountMultiplier .divide(breakValue,0,BigDecimal.ROUND_FLOOR); // (discountedQty)/breakQty
//						//System.out.println("discount_qty:aftr:"+discount_qty);
//					}
//					else
//					{
//						discount_qty= discount;
//					}
					
					discount_qty= discount;
					//System.out.println("discount_qty::"+discount_qty);
				}
				//System.out.println("discountQty:::::::::1::::::::"+discountQty);
				discountQty=discountQty.add(discount_qty);   
				//System.out.println("discountQty:::::::::2::::::::"+discountQty);
				discountCatagory="QQ";
				
				System.out.println("-----after---BD_QQ 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");			
	            break;
	          case 111:
	        	//BD_QV------Break Discount Quantity/Value
//	        	Price Breaks
	        	//System.out.println("-----b4------BD_QV 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");
	        	
	        	System.out.println("-----------BD_QV 1-----Price::"+Price+"--------");
	        	getBreaks(false);

	        	for (int i = 0; i < m_breaks.length; i++)
				{
					MDiscountSchemaBreak br = m_breaks[i];
					if (!br.isActive())
						continue;

					//Criteria apply
					if (!br.applies(Qty, M_Product_ID, M_Product_Category_ID))
					{
						//System.out.println("-----------BD_QV         NOT APPLICABLE-----Qty::"+Qty+"---");
						log.finer("No: " + br);
						continue;
					}
					else
					{
						//System.out.println("-----------BD_QV         APPLICABLE-----Qty::"+Qty+"---");
						//	Line applies
						//System.out.println("Line applies");
						//System.out.println("br.getBreakValue()"+br.getBreakValue());
						discountSchemaBreak_id=br.getM_DiscountSchemaBreak_ID(); //14 Jun 10
						breakValue=br.getBreakValue();
						
						if (br.isBPartnerFlatDiscount())
						{
							discount = BPartnerFlatDiscount;
							isPercentBased=BPartner_IsPercentBased;
						}
						else
						{
							discount = br.getBreakDiscount();
							isPercentBased=br.isPercentBased();
							IsProductBased=br.IsProductBased();//sysnova added
						}
					}
				}	//	for all breaks
	        	/*
				 * sysnova aded 24-14-2014
				 * for set discount with product base.
				 
				if(IsProductBased){
					//IsProductBased=br.IsProductBased();//sysnova added
					discountQty=Env.ZERO;   ;
					discountCatagory="QQ";
					break;
				}
				*/
				//end sysnova
	        	
				if(isPercentBased)
				{
//		    		multiplier = (onehundred).subtract(discount);
//	    			multiplier = multiplier.divide(onehundred, 6, BigDecimal.ROUND_HALF_UP);
//	    			Amt = Amt.multiply(multiplier);

					//System.out.println("-----------BD_QV 2---Price::"+Price+"---discount:"+discount+"-----discountVal::"+discountVal+"--");
		    		multiplier = (onehundred).subtract(discount);
		    		multiplier =multiplier.multiply(Amt);
		    		//Muntasim 11 May
	    			//Amt= multiplier.divide(onehundred, 6, BigDecimal.ROUND_HALF_UP);
		    		Amt= multiplier.divide(onehundred, 3, BigDecimal.ROUND_HALF_UP);
	    			//System.out.println("Amt"+Amt);
				}
				else
				{
					//System.out.println("-----------BD_QV 3---Price::"+Price+"---discount:"+discount+"-----discountVal::"+discountVal+"--");
					//my codee 3 aug
//					if(breakValue.compareTo(Env.ZERO)!=0)
//					{				
//						BigDecimal discountMultiplier = new BigDecimal(0);
//						/*
//						 *  (Amt)*discountValue/breakValue
//						 */
//						//System.out.println("discount:b4:"+discount+"discountedQty::"+discountedQty);
//						discountMultiplier =discount.multiply(discountedQty);
//						//System.out.println("discountMultiplier::"+discountMultiplier);					
//						//discount= discountMultiplier .divide(breakValue,0,BigDecimal.ROUND_HALF_DOWN); // (discountedQty)/breakQty
//						discount= discountMultiplier .divide(breakValue,2,BigDecimal.ROUND_FLOOR); // (discountedQty)/breakQty
//						//System.out.println("discount:aftr:"+discount);
//					}
//					else
//					{
//						//discount= discount;
//					}
					//System.out.println("Amt::"+Amt+"Qty::"+Qty+"discount::"+discount);
					
					//my codee 6 oct
					discount=discount.multiply(Qty);
					
					Amt=Amt.subtract(discount);
					//System.out.println("Amt::"+Amt);
				}
				//Muntasim 11 May
	        	//newPrice=Amt.divide(Qty, 6, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
				newPrice=Amt.divide(Qty, 3, BigDecimal.ROUND_HALF_UP);//new total amt ke Qty dia vaag kore new price ber kora hosse
	        	discountedPrice=newPrice;
	        	
	        	/***************/
	        	discount=Price.subtract(newPrice);
	    		convertedDiscountVal=getPercentedValue(Price, discount);
	    		discountVal=discountVal.add(convertedDiscountVal);
	        	/***************/

	    		/******/
	    		//System.out.println("Price::"+Price+"Qty::"+Qty+"Amt::"+Amt+"newPrice::"+newPrice);
	    		discountValWithoutPercent=(Price.multiply(Qty)).subtract(newPrice.multiply(Qty));
	    		//System.out.println("discountValWithoutPercent::"+discountValWithoutPercent);
	    		/*******/

	    		
				//System.out.println("-----------BD_QV 5--Price::"+Price+"----newPrice::"+newPrice+"----discountedPrice:"+discountedPrice);
				System.out.println("-----after------BD_QV 1---Price::"+Price+"--------discountedPrice::"+discountedPrice+"-----discount::"+discount+"-----discountVal:-"+discountVal+"-----discount_qty::"+discount_qty+"");
				
				discountCatagory="QV";
	            break;
	            
	            
	        default:
				//System.out.println("###################Default########################");
	      }//switch end
			
			discountVector.clear();
			
			
			System.out.println("---000000000000---discountedPrice::"+discountedPrice+"-------discountedQty::"+discountedQty+"-----discountVal::"+discountVal+"-----------discountQty::"+discountQty+"---"+"discountValWithoutPercent::"+discountValWithoutPercent);
			
			discountVector.add(0,""+discountedPrice);
			discountVector.add(1,""+discountedQty);
			discountVector.add(2,""+discountVal);
			discountVector.add(3,""+discountQty);																													
			discountVector.add(4,""+discountCatagory);
			discountVector.add(5,null);
			discountVector.add(6,""+discountValWithoutPercent);
			discountVector.add(7,null);//25 Jan 10
			discountVector.add(8,IsProductBased);//25 Jan 10 sysnova
			setM_DiscountSchemaBreak_ID(discountSchemaBreak_id); //14 Jun 10

			return discountVector;
		}	//	compute Discount

		/*
		 * 
		 *  get discount combinations 
		 * 
		 */
		public int getDiscountCombination()
		{
			int retVal=0;
			
			//System.out.println("getDiscountType():::::::"+getDiscountType());
			
			if (DISCOUNTTYPE_FlatPercent.equals(getDiscountType()))
			{
				if (isBPartnerFlatDiscount())
				{
					//FD_BP----------------Flat discount   Bussiness partner				
					retVal=11;
				}
				if (!isBPartnerFlatDiscount() && isPercentBased())
				{
					//FD_V_P-----------------Flat discount Value Based Percent
					retVal=01;
				}
				else if (!isBPartnerFlatDiscount() && !isPercentBased())
				{
					//FD_V_WP----------------Flat discount Value Based Without Percent
					retVal=00;
				}
			}
			else if (DISCOUNTTYPE_Breaks.equals(getDiscountType()))
			{
				if (!isQuantityBased() && !isMixed())
				{
					//BD_VV  ------Break Discount Value/Value
					retVal=100;
				}
				else if (!isQuantityBased() && isMixed())
				{
					//BD_VQ------Break Discount Value/Quantity 
					retVal=101;
				}
				else if (isQuantityBased() && !isMixed())
				{
					//BD_QQ------Break Discount Quantity/Quantity
					retVal=110;
				}
				else if (isQuantityBased() && isMixed())
				{
					//BD_QV------Break Discount Quantity/Value
					retVal=111;
				}
			}
			//System.out.println("isActive()::"+isActive());
			//discount schema is not active,so no discount combination
			if(!isActive())
			{
				retVal=-1;
			}
			return retVal;
		}

		
	/**
	 * 	getPercentedValue
	 *	@param BigDecimal price
	 *  @param BigDecimal discount
	 *	@return BigDecimal val
	 *   if price 300 then discount val 2
	 *              1-------		 2/300
	 *              100---------    (2/300)*100
	 *
	 *
	 */
		
	public BigDecimal getPercentedValue(BigDecimal price,BigDecimal discount)
	{
		BigDecimal val=Env.ZERO;
		BigDecimal onehundred = new BigDecimal(100);

		//System.out.println("price::"+price+"discount::"+discount);			
		BigDecimal multiplier=Env.ZERO;
		if(price.compareTo(Env.ZERO)>0)
		{	
			multiplier=discount;
			//Muntasim 11 May
			//multiplier = multiplier.divide(price, 6, BigDecimal.ROUND_HALF_DOWN);
			multiplier = multiplier.divide(price, 3, BigDecimal.ROUND_HALF_DOWN);
		}
		//System.out.println("multiplier::"+multiplier);
		val= multiplier.multiply(onehundred);
		//System.out.println("val::"+val);
		return val;
	}
		
	// Muntasim 281208 End
	// The original Calculate Discount method was overloaded to implement KFG discounts. 	
	////////////////////////////////////////////////////////////
	
	/**
	 * 	Is the Date in the Valid Range
	 *	@param date date
	 *	@return true if valid
	 *  @author Muntasim
	 */
	public boolean isDateValid (Timestamp date)
	{
		if (date == null)
			return false;
		/*
		if (getValidFrom() != null && date.before(getValidFrom()))
			return false;
		if (getValidTo() != null && date.after(getValidTo()))
			return false;
			*/
		return true;
	}	//	isDateValid
	//21 Jan 10
	public int getM_DiscountSchemaBreak_ID()
	{
		return m_DiscountSchemaBreak_ID;
	}
	//21 Jan 10
	public void setM_DiscountSchemaBreak_ID(int m_discountSchemaBreak_ID)
	{
		m_DiscountSchemaBreak_ID=m_discountSchemaBreak_ID;
	}
	/****sysnova********CNV-0001******End*********/
}	//	MDiscountSchema
