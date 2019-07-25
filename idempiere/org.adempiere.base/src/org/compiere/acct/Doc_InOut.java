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
package org.compiere.acct;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;

import org.compiere.model.I_C_OrderLine;
import org.compiere.model.I_M_InOutLine;
import org.compiere.model.I_M_RMALine;
import org.compiere.model.MAccount;
import org.compiere.model.MAcctSchema;
import org.compiere.model.MConversionRate;
import org.compiere.model.MCostDetail;
import org.compiere.model.MCurrency;
import org.compiere.model.MInOut;
import org.compiere.model.MInOutLine;
import org.compiere.model.MInOutLineMA;
import org.compiere.model.MOrderLandedCostAllocation;
import org.compiere.model.MOrderLine;
import org.compiere.model.MProduct;
import org.compiere.model.MTax;
import org.compiere.model.ProductCost;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Util;


/// sysnova added: start

import org.compiere.model.MCostVariance;
import org.compiere.model.MCostElement;
import org.compiere.model.MDocType;
import java.util.Vector;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.adempiere.exceptions.DBException;
import org.compiere.model.Query;
import java.util.Properties;
import java.util.Collection;
import java.util.List;
/// sysnova added: End
/**
 *  Post Shipment/Receipt Documents.
 *  <pre>
 *  Table:              M_InOut (319)
 *  Document Types:     MMS, MMR
 *  </pre>
 *  @author Jorg Janke
 *  @author Armen Rizal, Goodwill Consulting
 * 			<li>BF [ 1745154 ] Cost in Reversing Material Related Docs
 * 			<li>BF [ 2858043 ] Correct Included Tax in Average Costing
 *  @version  $Id: Doc_InOut.java,v 1.3 2006/07/30 00:53:33 jjanke Exp $
 */
public class Doc_InOut extends Doc
{
	/**
	 *  Constructor
	 * 	@param as accounting schema
	 * 	@param rs record
	 * 	@param trxName trx
	 */
	public Doc_InOut (MAcctSchema as, ResultSet rs, String trxName)
	{
		super (as, MInOut.class, rs, null, trxName);
	}   //  DocInOut

	private int				m_Reversal_ID = 0;
	@SuppressWarnings("unused")
	private String			m_DocStatus = "";
	private boolean 			m_deferPosting = false;

	/**
	 *  Load Document Details
	 *  @return error message or null
	 */
	protected String loadDocumentDetails()
	{
		setC_Currency_ID(NO_CURRENCY);
		MInOut inout = (MInOut)getPO();
		setDateDoc (inout.getMovementDate());
		m_Reversal_ID = inout.getReversal_ID();//store original (voided/reversed) document
		m_DocStatus = inout.getDocStatus();
		//	Contained Objects
		p_lines = loadLines(inout);
		if (log.isLoggable(Level.FINE)) log.fine("Lines=" + p_lines.length);

		if (inout.isSOTrx()) {
			MInOutLine[] lines = inout.getLines();
			for (MInOutLine line : lines) {
				I_C_OrderLine orderLine = line.getC_OrderLine();
				if (orderLine != null) {
					if (orderLine.getLink_OrderLine_ID() > 0) {
						//	Defer posting if found the linked MR is not posted
						String sql = "SELECT COUNT(*) FROM M_InOutLine iol WHERE iol.C_OrderLine_ID=? AND EXISTS (SELECT * FROM M_InOut io WHERE io.M_InOut_ID=iol.M_InOut_ID AND io.IsSOTrx='N' AND io.Posted<>'Y')";
						int count = DB.getSQLValueEx(getTrxName(), sql, orderLine.getLink_OrderLine_ID());
						if (count > 0) {
							m_deferPosting = true;
							break;
						}
					}
				}
			}
		}
		
		return null;
	}   //  loadDocumentDetails

	/**
	 *	Load InOut Line
	 *	@param inout shipment/receipt
	 *  @return DocLine Array
	 */
	private DocLine[] loadLines(MInOut inout)
	{
		ArrayList<DocLine> list = new ArrayList<DocLine>();
		MInOutLine[] lines = inout.getLines(false);
		for (int i = 0; i < lines.length; i++)
		{
			MInOutLine line = lines[i];
			if (line.isDescription()
				|| line.getM_Product_ID() == 0
				|| line.getMovementQty().signum() == 0)
			{
				if (log.isLoggable(Level.FINER)) log.finer("Ignored: " + line);
				continue;
			}

			DocLine docLine = new DocLine (line, this);
			BigDecimal Qty = line.getMovementQty();
			docLine.setReversalLine_ID(line.getReversalLine_ID());
			docLine.setQty (Qty, getDocumentType().equals(DOCTYPE_MatShipment));    //  sets Trx and Storage Qty

			//Define if Outside Processing
			String sql = "SELECT PP_Cost_Collector_ID  FROM C_OrderLine WHERE C_OrderLine_ID=? AND PP_Cost_Collector_ID IS NOT NULL";
			int PP_Cost_Collector_ID = DB.getSQLValueEx(getTrxName(), sql, new Object[]{line.getC_OrderLine_ID()});
			docLine.setPP_Cost_Collector_ID(PP_Cost_Collector_ID);
			//
			if (log.isLoggable(Level.FINE)) log.fine(docLine.toString());
			list.add (docLine);
		}

		//	Return Array
		DocLine[] dls = new DocLine[list.size()];
		list.toArray(dls);
		return dls;
	}	//	loadLines

	/**
	 *  Get Balance
	 *  @return Zero (always balanced)
	 */
	public BigDecimal getBalance()
	{
		BigDecimal retValue = Env.ZERO;
		return retValue;
	}   //  getBalance

	/**
	 *  Create Facts (the accounting logic) for
	 *  MMS, MMR.
	 *  <pre>
	 *  Shipment
	 *      CoGS (RevOrg)   DR
	 *      Inventory               CR
	 *  Shipment of Project Issue
	 *      CoGS            DR
	 *      Project                 CR
	 *  Receipt
	 *      Inventory       DR
	 *      NotInvoicedReceipt      CR
	 *  </pre>
	 *  @param as accounting schema
	 *  @return Fact
	 */
	public ArrayList<Fact> createFacts (MAcctSchema as)
	{
		//
		ArrayList<Fact> facts = new ArrayList<Fact>();
		//  create Fact Header
		Fact fact = new Fact(this, as, Fact.POST_Actual);
		setC_Currency_ID (as.getC_Currency_ID());

		//  Line pointers
		FactLine dr = null;
		FactLine cr = null;

		//  *** Sales - Shipment
		if (getDocumentType().equals(DOCTYPE_MatShipment) && isSOTrx())
		{
			for (int i = 0; i < p_lines.length; i++)
			{
				DocLine line = p_lines[i];				
				MProduct product = line.getProduct();
				BigDecimal costs = null;
				if (!isReversal(line))
				{
					if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as)) ) 
					{	
						if (line.getM_AttributeSetInstance_ID() == 0 ) 
						{
							MInOutLine ioLine = (MInOutLine) line.getPO();
							MInOutLineMA mas[] = MInOutLineMA.get(getCtx(), ioLine.get_ID(), getTrxName());
							if (mas != null && mas.length > 0 )
							{
								costs  = BigDecimal.ZERO;
								for (int j = 0; j < mas.length; j++)
								{
									MInOutLineMA ma = mas[j];
									BigDecimal QtyMA = ma.getMovementQty();
									ProductCost pc = line.getProductCost();
									pc.setQty(QtyMA);
									pc.setM_M_AttributeSetInstance_ID(ma.getM_AttributeSetInstance_ID());
									BigDecimal maCosts = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");
								
									costs = costs.add(maCosts);
								}						
							}
						} 
						else 
						{							
							costs = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");				
						}
					} 
					else
					{
						// MZ Goodwill
						// if Shipment CostDetail exist then get Cost from Cost Detail
						costs = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");			
					}
			
					// end MZ
					if (costs == null || costs.signum() == 0)	//	zero costs OK
					{
						if (product.isStocked())
						{
							//ok if we have purchased zero cost item from vendor before
							int count = DB.getSQLValue(null, "SELECT Count(*) FROM M_CostDetail WHERE M_Product_ID=? AND Processed='Y' AND Amt=0.00 AND Qty > 0 AND (C_OrderLine_ID > 0 OR C_InvoiceLine_ID > 0)", 
									product.getM_Product_ID());
							if (count > 0)
							{
								costs = BigDecimal.ZERO;
							}
							else
							{
								p_Error = "No Costs for " + line.getProduct().getName();
								log.log(Level.WARNING, p_Error);
								return null;
							}
						}
						else	//	ignore service
							continue;
					}
				}
				else
				{
					//temp to avoid NPE
					costs = BigDecimal.ZERO;
				}
				
				//  CoGS            DR
				dr = fact.createLine(line,
					line.getAccount(ProductCost.ACCTTYPE_P_Cogs, as),
					as.getC_Currency_ID(), costs, null);
				if (dr == null)
				{
					p_Error = "FactLine DR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				dr.setM_Locator_ID(line.getM_Locator_ID());
				dr.setLocationFromLocator(line.getM_Locator_ID(), true);    //  from Loc
				dr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
				dr.setAD_Org_ID(line.getOrder_Org_ID());		//	Revenue X-Org
				dr.setQty(line.getQty().negate());
				
				if (isReversal(line))
				{
					//	Set AmtAcctDr from Original Shipment/Receipt
					if (!dr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						if (! product.isStocked())	{ //	ignore service
							fact.remove(dr);
							continue;
						}
						p_Error = "Original Shipment/Receipt not posted yet";
						return null;
					}
				}

				//  Inventory               CR
				cr = fact.createLine(line,
					line.getAccount(ProductCost.ACCTTYPE_P_Asset, as),
					as.getC_Currency_ID(), null, costs);
				if (cr == null)
				{
					p_Error = "FactLine CR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				cr.setM_Locator_ID(line.getM_Locator_ID());
				cr.setLocationFromLocator(line.getM_Locator_ID(), true);    // from Loc
				cr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  // to Loc
				
				if (isReversal(line))
				{
					//	Set AmtAcctCr from Original Shipment/Receipt
					if (!cr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						p_Error = "Original Shipment/Receipt not posted yet";
						return null;
					}
					costs = cr.getAcctBalance(); //get original cost
				}
				if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as)) ) 
				{	
					if (line.getM_AttributeSetInstance_ID() == 0 ) 
					{
						MInOutLine ioLine = (MInOutLine) line.getPO();
						MInOutLineMA mas[] = MInOutLineMA.get(getCtx(), ioLine.get_ID(), getTrxName());
						if (mas != null && mas.length > 0 )
						{
							for (int j = 0; j < mas.length; j++)
							{
								MInOutLineMA ma = mas[j];
								if (!MCostDetail.createShipment(as, line.getAD_Org_ID(),
										line.getM_Product_ID(), ma.getM_AttributeSetInstance_ID(),
										line.get_ID(), 0,
										costs, ma.getMovementQty().negate(),
										line.getDescription(), true, getTrxName()))
								{
									p_Error = "Failed to create cost detail record";
									return null;
								}							
							}						
						}
					} 
					else
					{
						//
						if (line.getM_Product_ID() != 0)
						{
							if (!MCostDetail.createShipment(as, line.getAD_Org_ID(),
								line.getM_Product_ID(), line.getM_AttributeSetInstance_ID(),
								line.get_ID(), 0,
								costs, line.getQty(),
								line.getDescription(), true, getTrxName()))
							{
								p_Error = "Failed to create cost detail record";
								return null;
							}
						}
					}
				} 
				else
				{
					//
					if (line.getM_Product_ID() != 0)
					{
						if (!MCostDetail.createShipment(as, line.getAD_Org_ID(),
							line.getM_Product_ID(), line.getM_AttributeSetInstance_ID(),
							line.get_ID(), 0,
							costs, line.getQty(),
							line.getDescription(), true, getTrxName()))
						{
							p_Error = "Failed to create cost detail record";
							return null;
						}
					}
				}
				
				// sysnova ****Added********SL-0001******Added********* 
				BigDecimal totalCost=Env.ZERO;
				if (MAcctSchema.COSTINGMETHOD_StandardCosting.equals(as.getCostingMethod()))
				{
					MCostVariance mCostVariance=new MCostVariance (Env.getCtx(), 0, getTrxName());	
					ArrayList VarianceData=mCostVariance.findVarianceDetails(line.getAD_Org_ID(), line.getM_Product_ID(),line.getM_AttributeSetInstance_ID(),
							line.get_ID(),"M_InOutLine_ID", getTrxName());
					for (int ii = 0; ii < VarianceData.size(); ii++)
					{
						Vector vLine=(Vector)VarianceData.get(ii);
						int T_costVariancedetail_ID  = ((Integer)vLine.get(0)).intValue();
						if(T_costVariancedetail_ID<1){
							continue;
						}
						//MAccount acct=null;
						BigDecimal postAmt=Env.ZERO;
						
						int M_CostElement_ID  = ((Integer)vLine.get(1)).intValue();
						if(M_CostElement_ID<1){
							continue;
						}
						
						
						BigDecimal currentqty=(BigDecimal)vLine.get(2);
						BigDecimal currentcostprice=(BigDecimal)vLine.get(3);
						postAmt=currentcostprice.multiply(line.getQty().negate());
						MCostElement element = MCostElement.get(getCtx(), M_CostElement_ID);
						int m_costElementvariance_ID=getcostElementReverse_ID(M_CostElement_ID, as, getTrxName());
						int C_ValidCombination_ID=Doc_InOut.getC_ValidCombinationCostVariance (m_costElementvariance_ID, as, getTrxName());
						if(C_ValidCombination_ID>0){
							MAccount acct = MAccount.get (as.getCtx(), C_ValidCombination_ID);
						}else{
							p_Error = " Cost Element variance_ID Not Found For m_costelement : "+element.getName();
							return null;
						}
						int C_ValidCombination_IDCOGS=Doc_InOut.getC_ValidCombinationCOGSVariance (m_costElementvariance_ID, as, getTrxName());
						//X_T_CostVariance T_costVariance=new X_T_CostVariance (getCtx(),M_CostElement_ID, getTrxName());
						//int C_ValidCombination_ID=getC_ValidCombinationVariance_acct(T_costVariance.getM_CostElement_ID(), as, getTrxName());
						if(C_ValidCombination_ID<1){
							p_Error = "C_ValidCombination_ID Not Found For M_CostElement_ID="+M_CostElement_ID;
							log.log(Level.WARNING, p_Error);
							return null;
						}
						if(C_ValidCombination_IDCOGS<1){
							p_Error = "cogsvar_acct Not Found For M_CostElement_ID="+M_CostElement_ID;
							log.log(Level.WARNING, p_Error);
							return null;
						}
						if(C_ValidCombination_ID>0 && C_ValidCombination_IDCOGS>0)
						{
							MAccount acct = MAccount.get (as.getCtx(), C_ValidCombination_ID);
							MAccount COGSacct = MAccount.get (as.getCtx(), C_ValidCombination_IDCOGS);
							FactLine drr =null;
							FactLine crr =null;
							if(postAmt.signum()>0)
								//drr = fact.createLine(line, line.getAccount(ProductCost.ACCTTYPE_P_Cogs, as),as.getC_Currency_ID(), postAmt, null);
								drr = fact.createLine(line, COGSacct,as.getC_Currency_ID(), postAmt, null);
								
							else{
								//drr = fact.createLine(line, line.getAccount(ProductCost.ACCTTYPE_P_Cogs, as),as.getC_Currency_ID(),  null,postAmt.abs());
								drr = fact.createLine(line, COGSacct,as.getC_Currency_ID(),  null,postAmt.abs());
							}
							if (drr == null)
							{
								p_Error = "dr not created: " + line;
								log.log(Level.WARNING, p_Error);
								return null;
							}
							drr.setM_Locator_ID(line.getM_Locator_ID());
							drr.setLocationFromLocator(line.getM_Locator_ID(), true);    //  from Loc
							drr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
							drr.setAD_Org_ID(line.getOrder_Org_ID());		//	Revenue X-Org
							drr.setQty(line.getQty().negate());
							
							
							if(postAmt.signum()>0)
								crr = fact.createLine(line,acct,as.getC_Currency_ID(), null, postAmt);
							else 
								crr = fact.createLine(line,acct,as.getC_Currency_ID(), postAmt.abs(), null);
							//
							if (crr == null)
							{
								p_Error = "CR not created: " + line;
								log.log(Level.WARNING, p_Error);
								return null;
							}
							crr.setM_Locator_ID(line.getM_Locator_ID());
							crr.setLocationFromLocator(line.getM_Locator_ID(), true);    // from Loc
							crr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  // to Loc
						}
						else{
							p_Error = "C_ValidCombination_ID Not Found For M_CostElement_ID ";
							log.log(Level.WARNING, p_Error);
							return null;
						}
					}
				}
				/* sysnova ****END********SL-0001******END*********/
			}	//	for all lines

			/** Commitment release										****/
			if (as.isAccrual() && as.isCreateSOCommitment())
			{
				for (int i = 0; i < p_lines.length; i++)
				{
					DocLine line = p_lines[i];
					Fact factcomm = Doc_Order.getCommitmentSalesRelease(as, this,
						line.getQty(), line.get_ID(), Env.ONE);
					if (factcomm != null)
						facts.add(factcomm);
				}
			}	//	Commitment

		}	//	Shipment
        //	  *** Sales - Return
		else if ( getDocumentType().equals(DOCTYPE_MatReceipt) && isSOTrx() )
		{
			for (int i = 0; i < p_lines.length; i++)
			{
				DocLine line = p_lines[i];
				MProduct product = line.getProduct();
				BigDecimal costs = null;
				if (!isReversal(line)) 
				{
					if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as)) ) 
					{	
						if (line.getM_AttributeSetInstance_ID() == 0 ) 
						{
							MInOutLine ioLine = (MInOutLine) line.getPO();
							MInOutLineMA mas[] = MInOutLineMA.get(getCtx(), ioLine.get_ID(), getTrxName());
							costs = BigDecimal.ZERO;
							if (mas != null && mas.length > 0 )
							{
								for (int j = 0; j < mas.length; j++)
								{
									MInOutLineMA ma = mas[j];
									BigDecimal QtyMA = ma.getMovementQty();
									ProductCost pc = line.getProductCost();
									pc.setQty(QtyMA);
									pc.setM_M_AttributeSetInstance_ID(ma.getM_AttributeSetInstance_ID());
									BigDecimal maCosts = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");
								
									costs = costs.add(maCosts);
								}
							}
						} 
						else
						{
							costs = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");
						}
					}
					else
					{
						// MZ Goodwill
						// if Shipment CostDetail exist then get Cost from Cost Detail
						costs = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");
						// end MZ
					}
					if (costs == null || costs.signum() == 0)	//	zero costs OK
					{
						if (product.isStocked())
						{
							p_Error = "No Costs for " + line.getProduct().getName();
							log.log(Level.WARNING, p_Error);
							return null;
						}
						else	//	ignore service
							continue;
					}
				} 
				else
				{
					costs = BigDecimal.ZERO;
				}
				//  Inventory               DR
				dr = fact.createLine(line,
					line.getAccount(ProductCost.ACCTTYPE_P_Asset, as),
					as.getC_Currency_ID(), costs, null);
				if (dr == null)
				{
					p_Error = "FactLine DR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				dr.setM_Locator_ID(line.getM_Locator_ID());
				dr.setLocationFromLocator(line.getM_Locator_ID(), true);    // from Loc
				dr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  // to Loc
				if (isReversal(line))
				{
					//	Set AmtAcctDr from Original Shipment/Receipt
					if (!dr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						if (! product.isStocked())	{ //	ignore service
							fact.remove(dr);
							continue;
						}
						p_Error = "Original Shipment/Receipt not posted yet";
						return null;
					}
					costs = dr.getAcctBalance(); //get original cost
				}
				//
				if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as)) ) 
				{	
					if (line.getM_AttributeSetInstance_ID() == 0 ) 
					{
						MInOutLine ioLine = (MInOutLine) line.getPO();
						MInOutLineMA mas[] = MInOutLineMA.get(getCtx(), ioLine.get_ID(), getTrxName());
						if (mas != null && mas.length > 0 )
						{
							for (int j = 0; j < mas.length; j++)
							{
								MInOutLineMA ma = mas[j];
								if (!MCostDetail.createShipment(as, line.getAD_Org_ID(),
										line.getM_Product_ID(), ma.getM_AttributeSetInstance_ID(),
										line.get_ID(), 0,
										costs, ma.getMovementQty(),
										line.getDescription(), true, getTrxName()))
								{
									p_Error = "Failed to create cost detail record";
									return null;
								}
							}
						}
					} else
					{
						if (line.getM_Product_ID() != 0)
						{
							if (!MCostDetail.createShipment(as, line.getAD_Org_ID(),
								line.getM_Product_ID(), line.getM_AttributeSetInstance_ID(),
								line.get_ID(), 0,
								costs, line.getQty(),
								line.getDescription(), true, getTrxName()))
							{
								p_Error = "Failed to create cost detail record";
								return null;
							}
						}
					}
				} else
				{
					//
					if (line.getM_Product_ID() != 0)
					{
						if (!MCostDetail.createShipment(as, line.getAD_Org_ID(),
							line.getM_Product_ID(), line.getM_AttributeSetInstance_ID(),
							line.get_ID(), 0,
							costs, line.getQty(),
							line.getDescription(), true, getTrxName()))
						{
							p_Error = "Failed to create cost detail record";
							return null;
						}
					}
				}

				//  CoGS            CR
				cr = fact.createLine(line,
					line.getAccount(ProductCost.ACCTTYPE_P_Cogs, as),
					as.getC_Currency_ID(), null, costs);
				if (cr == null)
				{
					p_Error = "FactLine CR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				cr.setM_Locator_ID(line.getM_Locator_ID());
				cr.setLocationFromLocator(line.getM_Locator_ID(), true);    //  from Loc
				cr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
				cr.setAD_Org_ID(line.getOrder_Org_ID());		//	Revenue X-Org
				cr.setQty(line.getQty().negate());
				if (isReversal(line))
				{
					//	Set AmtAcctCr from Original Shipment/Receipt
					if (!cr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						p_Error = "Original Shipment/Receipt not posted yet";
						return null;
					}
				}

				/****sysnova**************Added*********/
				/**
				 * For return variance
				 * 
				 */
				MDocType dt = MDocType.get(getCtx(), getC_DocType_ID());
				if(dt.getName().equals("MM Returns")){
					BigDecimal totalCost=Env.ZERO;
					if (MAcctSchema.COSTINGMETHOD_StandardCosting.equals(as.getCostingMethod()))
					{
						MCostVariance mCostVariance=new MCostVariance (Env.getCtx(), 0, getTrxName());	
						ArrayList VarianceData=mCostVariance.findVarianceDetails(line.getAD_Org_ID(), line.getM_Product_ID(),line.getM_AttributeSetInstance_ID(),
								line.get_ID(),"M_InOutLine_ID", getTrxName());
						for (int ii = 0; ii < VarianceData.size(); ii++)
						{
							Vector vLine=(Vector)VarianceData.get(ii);
							int T_costVariancedetail_ID  = ((Integer)vLine.get(0)).intValue();
							if(T_costVariancedetail_ID<1){
								continue;
							}
							//MAccount acct=null;
							BigDecimal postAmt=Env.ZERO;
							int M_CostElement_ID  = ((Integer)vLine.get(1)).intValue();
							if(M_CostElement_ID<1){
								continue;
							}
							BigDecimal currentqty=(BigDecimal)vLine.get(2);
							BigDecimal currentcostprice=(BigDecimal)vLine.get(3);
							postAmt=currentcostprice.multiply(line.getQty().negate());
							MCostElement element = MCostElement.get(getCtx(), M_CostElement_ID);
							int m_costElementvariance_ID=getcostElementReverse_ID(M_CostElement_ID, as, getTrxName());
							int C_ValidCombination_ID=Doc_InOut.getC_ValidCombinationCostVariance (m_costElementvariance_ID, as, getTrxName());
							if(C_ValidCombination_ID>0){
								MAccount acct = MAccount.get (as.getCtx(), C_ValidCombination_ID);
							}else{
								p_Error = " Cost Element variance_ID Not Found For m_costelement : "+element.getName();
								return null;
							}
							//X_T_CostVariance T_costVariance=new X_T_CostVariance (getCtx(),M_CostElement_ID, getTrxName());
							//int C_ValidCombination_ID=getC_ValidCombinationVariance_acct(T_costVariance.getM_CostElement_ID(), as, getTrxName());
							if(C_ValidCombination_ID>0)
							{								
								MAccount acct = MAccount.get (as.getCtx(), C_ValidCombination_ID);
								FactLine drr =null;
								FactLine crr =null;
								int C_ValidCombinationcogs_ID=Doc_InOut.getC_ValidCombinationCOGSVariance (element.get_ID(), as, getTrxName());
								if(C_ValidCombinationcogs_ID<1){
									p_Error = "C_ValidCombinationcogs_ID Not Found For M_CostElement "+element.getName();
									log.log(Level.WARNING, p_Error);
									return null;
								}
								MAccount acctdr = MAccount.get (as.getCtx(), C_ValidCombinationcogs_ID);
								if(postAmt.signum()>0)
									drr = fact.createLine(line, acctdr,as.getC_Currency_ID(), postAmt, null);
								else{
									drr = fact.createLine(line, acctdr,as.getC_Currency_ID(),  null,postAmt.abs());
								}
								if (drr == null)
								{
									p_Error = "dr not created: " + line;
									log.log(Level.WARNING, p_Error);
									return null;
								}
								drr.setM_Locator_ID(line.getM_Locator_ID());
								drr.setLocationFromLocator(line.getM_Locator_ID(), true);    //  from Loc
								drr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
								drr.setAD_Org_ID(line.getOrder_Org_ID());		//	Revenue X-Org
								drr.setQty(line.getQty().negate());
								if(postAmt.signum()>0)
									crr = fact.createLine(line,acct,as.getC_Currency_ID(), null, postAmt);
								else 
									crr = fact.createLine(line,acct,as.getC_Currency_ID(), postAmt.abs(), null);
								//
								if (crr == null)
								{
									p_Error = "CR not created: " + line;
									log.log(Level.WARNING, p_Error);
									return null;
								}
								crr.setM_Locator_ID(line.getM_Locator_ID());
								crr.setLocationFromLocator(line.getM_Locator_ID(), true);    // from Loc
								crr.setLocationFromBPartner(getC_BPartner_Location_ID(), false);  // to Loc
							}
							else{
								p_Error = "C_ValidCombination_ID Not Found For M_CostElement "+element.getName();
								log.log(Level.WARNING, p_Error);
								return null;
							}
						}
					}
					
				}
				/****sysnova**************Ended*********/
				
			}	//	for all lines
		}	//	Sales Return

		//  *** Purchasing - Receipt
		else if (getDocumentType().equals(DOCTYPE_MatReceipt) && !isSOTrx())
		{
			for (int i = 0; i < p_lines.length; i++)
			{
				// Elaine 2008/06/26
				int C_Currency_ID = as.getC_Currency_ID();
				//
				DocLine line = p_lines[i];
				BigDecimal costs = null;
				MProduct product = line.getProduct();
				MOrderLine orderLine = null;
				BigDecimal landedCost = BigDecimal.ZERO;
				String costingMethod = product.getCostingMethod(as);
				if (!isReversal(line))
				{					
					int C_OrderLine_ID = line.getC_OrderLine_ID();
					if (C_OrderLine_ID > 0)
					{
						orderLine = new MOrderLine (getCtx(), C_OrderLine_ID, getTrxName());
						MOrderLandedCostAllocation[] allocations = MOrderLandedCostAllocation.getOfOrderLine(C_OrderLine_ID, getTrxName());
						for(MOrderLandedCostAllocation allocation : allocations) 
						{														
							BigDecimal totalAmt = allocation.getAmt();
							BigDecimal totalQty = allocation.getQty();
							BigDecimal amt = totalAmt.multiply(line.getQty()).divide(totalQty, RoundingMode.HALF_UP);
							landedCost = landedCost.add(amt);							
						}
					}
															
					//get costing method for product					
					if (MAcctSchema.COSTINGMETHOD_AveragePO.equals(costingMethod) ||
						MAcctSchema.COSTINGMETHOD_AverageInvoice.equals(costingMethod) ||
						MAcctSchema.COSTINGMETHOD_LastPOPrice.equals(costingMethod)  ||
						( MAcctSchema.COSTINGMETHOD_StandardCosting.equals(costingMethod) &&  MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as))))
					{
						// Low - check if c_orderline_id is valid
						if (orderLine != null)
						{
						    // Elaine 2008/06/26
						    C_Currency_ID = orderLine.getC_Currency_ID();
						    //
						    costs = orderLine.getPriceCost();
						    if (costs == null || costs.signum() == 0)
						    {
						    	costs = orderLine.getPriceActual();
								//	Goodwill: Correct included Tax
						    	int C_Tax_ID = orderLine.getC_Tax_ID();
								if (orderLine.isTaxIncluded() && C_Tax_ID != 0)
								{
									MTax tax = MTax.get(getCtx(), C_Tax_ID);
									if (!tax.isZeroTax())
									{
										int stdPrecision = MCurrency.getStdPrecision(getCtx(), C_Currency_ID);
										BigDecimal costTax = tax.calculateTax(costs, true, stdPrecision);
										if (log.isLoggable(Level.FINE)) log.fine("Costs=" + costs + " - Tax=" + costTax);
										costs = costs.subtract(costTax);
									}
								}	//	correct included Tax
						    }
						    costs = costs.multiply(line.getQty());
	                    }
	                    else
	                    {	                    	
	                    	p_Error = "Resubmit - No Costs for " + product.getName() + " (required order line)";
	                        log.log(Level.WARNING, p_Error);
	                        return null;
	                    }
	                    //
					}
					else
					{
						costs = line.getProductCosts(as, line.getAD_Org_ID(), false);	//	current costs
					}
					
					if (costs == null || costs.signum() == 0)
					{
						//ok if purchase price is actually zero 
						if (orderLine != null && orderLine.getPriceActual().signum() == 0)
                    	{
							costs = BigDecimal.ZERO;
                    	}
						else
						{
							p_Error = "Resubmit - No Costs for " + product.getName();
							log.log(Level.WARNING, p_Error);
							return null;
						}
					}										
				} 
				else
				{
					costs = BigDecimal.ZERO;
				}
				
				//  Inventory/Asset			DR
				MAccount assets = line.getAccount(ProductCost.ACCTTYPE_P_Asset, as);
				if (product.isService())
				{
					//if the line is a Outside Processing then DR WIP
					if(line.getPP_Cost_Collector_ID() > 0)
						assets = line.getAccount(ProductCost.ACCTTYPE_P_WorkInProcess, as);
					else
						assets = line.getAccount(ProductCost.ACCTTYPE_P_Expense, as);

				}

				BigDecimal drAsset = costs;
				if (landedCost.signum() != 0 && (MAcctSchema.COSTINGMETHOD_AverageInvoice.equals(costingMethod)
					|| MAcctSchema.COSTINGMETHOD_AveragePO.equals(costingMethod)))
				{
					drAsset = drAsset.add(landedCost);
				}
				dr = fact.createLine(line, assets,
					C_Currency_ID, drAsset, null);
				//
				if (dr == null)
				{
					p_Error = "DR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				dr.setM_Locator_ID(line.getM_Locator_ID());
				dr.setLocationFromBPartner(getC_BPartner_Location_ID(), true);   // from Loc
				dr.setLocationFromLocator(line.getM_Locator_ID(), false);   // to Loc
				if (isReversal(line))
				{
					//	Set AmtAcctDr from Original Shipment/Receipt
					if (!dr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						if (! product.isStocked())	{ //	ignore service
							fact.remove(dr);
							continue;
						}
						p_Error = "Original Receipt not posted yet";
						return null;
					}
				}

				//  NotInvoicedReceipt				CR
				
				//* sysnova ***Added********SL-0001******Added********* 
				//cr = fact.createLine(line, //blocked original
				//		getAccount(Doc.ACCTTYPE_NotInvoicedReceipts, as),
				//		C_Currency_ID, null, costs);
				ArrayList costElement = new ArrayList();
				if (MAcctSchema.COSTINGMETHOD_StandardCosting.equals(costingMethod))
				{
					landedCost=Env.ZERO;
					costElement=getCurrentCost (product,0, 
							as, line.getOrder_Org_ID(),as.getM_CostType_ID(), costingMethod,
							line.getQty(), 0, true, getTrxName());
					if(costElement!=null){
						for (int j = 0; j < costElement.size(); j++){
							Vector costElementData = (Vector) costElement.get(j);
							BigDecimal amt= (BigDecimal) costElementData.get(1);
							landedCost = landedCost.add(amt);	
						}
					}
					landedCost = landedCost.multiply(line.getQty());
					cr = fact.createLine(line,
							getAccount(Doc.ACCTTYPE_NotInvoicedReceipts, as),
							C_Currency_ID, null, costs.subtract(landedCost));
				}
				
				else
					cr = fact.createLine(line,
						getAccount(Doc.ACCTTYPE_NotInvoicedReceipts, as),
						C_Currency_ID, null, costs);
				// sysnova ****END********SL-0001******END*********
				
				
				
//				cr = fact.createLine(line,
//					getAccount(Doc.ACCTTYPE_NotInvoicedReceipts, as),
//					C_Currency_ID, null, costs);
				//
				if (cr == null)
				{
					p_Error = "CR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				cr.setM_Locator_ID(line.getM_Locator_ID());
				cr.setLocationFromBPartner(getC_BPartner_Location_ID(), true);   //  from Loc
				cr.setLocationFromLocator(line.getM_Locator_ID(), false);   //  to Loc
				cr.setQty(line.getQty().negate());
				if (isReversal(line))
				{
					//	Set AmtAcctCr from Original Shipment/Receipt
					if (!cr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						p_Error = "Original Receipt not posted yet";
						return null;
					}
				}
				

				// sysnova ****Added********SL-0001******Added********* 
				if (MAcctSchema.COSTINGMETHOD_StandardCosting.equals(costingMethod))
				{
					if(costElement!=null){
						for (int j = 0; j < costElement.size(); j++){
							Vector costElementData = (Vector) costElement.get(j);
							int costElementData_ID = Integer.parseInt(""+costElementData.get(0));
							BigDecimal amt= (BigDecimal) costElementData.get(1);
							BigDecimal totalamt=amt.multiply(line.getQty());
							int C_ValidCombination_ID=getC_ValidCombination (costElementData_ID, as, getTrxName());
							if(C_ValidCombination_ID>0){
								MAccount acct = MAccount.get (as.getCtx(), C_ValidCombination_ID);
								
								cr = fact.createLine(line,
										acct,C_Currency_ID, null, totalamt);
								//
								if (cr == null)
								{
									p_Error = "CR not created: " + line;
									log.log(Level.WARNING, p_Error);
									return null;
								}
								cr.setM_Locator_ID(line.getM_Locator_ID());
								cr.setLocationFromBPartner(getC_BPartner_Location_ID(), true);   //  from Loc
								cr.setLocationFromLocator(line.getM_Locator_ID(), false);   //  to Loc
								cr.setQty(line.getQty().negate());
							}
						}
					}
				}
				// sysnova ****END********SL-0001******END*********
				
				
				if (!fact.isAcctBalanced())
				{
					if (isReversal(line))
					{
						dr = fact.createLine(line,
								line.getAccount(ProductCost.ACCTTYPE_P_LandedCostClearing, as),
								C_Currency_ID, Env.ONE, (BigDecimal)null);
						if (!dr.updateReverseLine (MInOut.Table_ID,
								m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
						{
							p_Error = "Original Receipt not posted yet";
							return null;
						}
					}
					else if (landedCost.signum() != 0)
					{
						cr = fact.createLine(line,
								line.getAccount(ProductCost.ACCTTYPE_P_LandedCostClearing, as),
								C_Currency_ID, null, landedCost);
						//
						if (cr == null)
						{
							p_Error = "CR not created: " + line;
							log.log(Level.WARNING, p_Error);
							return null;
						}
						cr.setM_Locator_ID(line.getM_Locator_ID());
						cr.setLocationFromBPartner(getC_BPartner_Location_ID(), true);   //  from Loc
						cr.setLocationFromLocator(line.getM_Locator_ID(), false);   //  to Loc
						cr.setQty(line.getQty().negate());
					}
				}
			}
		}	//	Receipt
         //	  *** Purchasing - return
		else if (getDocumentType().equals(DOCTYPE_MatShipment) && !isSOTrx())
		{
			for (int i = 0; i < p_lines.length; i++)
			{
				// Elaine 2008/06/26
				int C_Currency_ID = as.getC_Currency_ID();
				//
				DocLine line = p_lines[i];
				BigDecimal costs = null;
				MProduct product = line.getProduct();
				if (!isReversal(line))
				{
					MInOutLine ioLine = (MInOutLine) line.getPO();
					I_M_RMALine rmaLine = ioLine.getM_RMALine();
					costs = rmaLine != null ? rmaLine.getAmt() : BigDecimal.ZERO;
					I_M_InOutLine originalInOutLine = rmaLine != null ? rmaLine.getM_InOutLine() : null;
					if (originalInOutLine != null && originalInOutLine.getC_OrderLine_ID() > 0)
					{
						MOrderLine originalOrderLine = (MOrderLine) originalInOutLine.getC_OrderLine();
						//	Goodwill: Correct included Tax
				    	int C_Tax_ID = originalOrderLine.getC_Tax_ID();
				    	if (originalOrderLine.isTaxIncluded() && C_Tax_ID != 0)
						{
							MTax tax = MTax.get(getCtx(), C_Tax_ID);
							if (!tax.isZeroTax())
							{
								int stdPrecision = MCurrency.getStdPrecision(getCtx(), originalOrderLine.getC_Currency_ID());
								BigDecimal costTax = tax.calculateTax(costs, true, stdPrecision);
								if (log.isLoggable(Level.FINE)) log.fine("Costs=" + costs + " - Tax=" + costTax);
								costs = costs.subtract(costTax);
							}
						}	//	correct included Tax
				    	
				    	// different currency
				    	if (C_Currency_ID  != originalOrderLine.getC_Currency_ID()) 
						{
							costs = MConversionRate.convert (getCtx(),
									costs, originalOrderLine.getC_Currency_ID(), C_Currency_ID,
									getDateAcct(), 0, getAD_Client_ID(), getAD_Org_ID(), true);
						}

				    	costs = costs.multiply(line.getQty());
				    	costs = costs.negate();
					}
					else
					{
						if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as)) ) 
						{	
							if (line.getM_AttributeSetInstance_ID() == 0 ) 
							{								
								MInOutLineMA mas[] = MInOutLineMA.get(getCtx(), ioLine.get_ID(), getTrxName());
								costs = BigDecimal.ZERO;
								if (mas != null && mas.length > 0 )
								{
									for (int j = 0; j < mas.length; j++)
									{
										MInOutLineMA ma = mas[j];
										BigDecimal QtyMA = ma.getMovementQty();
										ProductCost pc = line.getProductCost();
										pc.setQty(QtyMA);
										pc.setM_M_AttributeSetInstance_ID(ma.getM_AttributeSetInstance_ID());
										BigDecimal maCosts = line.getProductCosts(as, line.getAD_Org_ID(), true, "M_InOutLine_ID=?");
									
										costs = costs.add(maCosts);
									}						
								}
							} 
							else
							{
								costs = line.getProductCosts(as, line.getAD_Org_ID(), false);	//	current costs
							}						
						} 
						else
						{
							costs = line.getProductCosts(as, line.getAD_Org_ID(), false);	//	current costs
						}
						
						if (costs == null || costs.signum() == 0)
						{
							p_Error = "Resubmit - No Costs for " + product.getName();
							log.log(Level.WARNING, p_Error);
							return null;
						}
					}
				}
				else
				{
					//update below
					costs = Env.ONE;
				}
				//  NotInvoicedReceipt				DR
				// Elaine 2008/06/26
				/*dr = fact.createLine(line,
					getAccount(Doc.ACCTTYPE_NotInvoicedReceipts, as),
					as.getC_Currency_ID(), costs , null);*/
				dr = fact.createLine(line,
					getAccount(Doc.ACCTTYPE_NotInvoicedReceipts, as),
					C_Currency_ID, costs , null);
				//
				if (dr == null)
				{
					p_Error = "CR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				dr.setM_Locator_ID(line.getM_Locator_ID());
				dr.setLocationFromBPartner(getC_BPartner_Location_ID(), true);   //  from Loc
				dr.setLocationFromLocator(line.getM_Locator_ID(), false);   //  to Loc
				dr.setQty(line.getQty().negate());
				if (isReversal(line))
				{
					//	Set AmtAcctDr from Original Shipment/Receipt
					if (!dr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						if (! product.isStocked())	{ //	ignore service
							fact.remove(dr);
							continue;
						}
						p_Error = "Original Receipt not posted yet";
						return null;
					}
				}

				//  Inventory/Asset			CR
				MAccount assets = line.getAccount(ProductCost.ACCTTYPE_P_Asset, as);
				if (product.isService())
					assets = line.getAccount(ProductCost.ACCTTYPE_P_Expense, as);
				// Elaine 2008/06/26
				/*cr = fact.createLine(line, assets,
					as.getC_Currency_ID(), null, costs);*/
				cr = fact.createLine(line, assets,
					C_Currency_ID, null, costs);
				//
				if (cr == null)
				{
					p_Error = "DR not created: " + line;
					log.log(Level.WARNING, p_Error);
					return null;
				}
				cr.setM_Locator_ID(line.getM_Locator_ID());
				cr.setLocationFromBPartner(getC_BPartner_Location_ID(), true);   // from Loc
				cr.setLocationFromLocator(line.getM_Locator_ID(), false);   // to Loc
				if (isReversal(line))
				{
					//	Set AmtAcctCr from Original Shipment/Receipt
					if (!cr.updateReverseLine (MInOut.Table_ID,
							m_Reversal_ID, line.getReversalLine_ID(),Env.ONE))
					{
						p_Error = "Original Receipt not posted yet";
						return null;
					}
				}
				
				String costingError = createVendorRMACostDetail(as, line, costs);
				if (!Util.isEmpty(costingError))
				{
					p_Error = costingError;
					return null;
				}
			}
						
		}	//	Purchasing Return
		else
		{
			p_Error = "DocumentType unknown: " + getDocumentType();
			log.log(Level.SEVERE, p_Error);
			return null;
		}
		//
		facts.add(fact);
		return facts;
	}   //  createFact

	private boolean isReversal(DocLine line) {
		return m_Reversal_ID !=0 && line.getReversalLine_ID() != 0;
	}

	private String createVendorRMACostDetail(MAcctSchema as, DocLine line, BigDecimal costs)
	{		
		BigDecimal tQty = line.getQty();
		BigDecimal tAmt = costs;
		if (tAmt.signum() != tQty.signum())
		{
			tAmt = tAmt.negate();
		}
		MProduct product = line.getProduct();
		if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(product.getCostingLevel(as)) ) 
		{	
			if (line.getM_AttributeSetInstance_ID() == 0 ) 
			{								
				MInOutLineMA mas[] = MInOutLineMA.get(getCtx(), line.get_ID(), getTrxName());
				if (mas != null && mas.length > 0 )
				{
					for (int j = 0; j < mas.length; j++)
					{
						MInOutLineMA ma = mas[j];
						if (!MCostDetail.createShipment(as, line.getAD_Org_ID(), line.getM_Product_ID(), 
								ma.getM_AttributeSetInstance_ID(), line.get_ID(), 0, tAmt, ma.getMovementQty().negate(), 
								line.getDescription(), false, getTrxName()))
							return "SaveError";
					}
				}
			} 
			else
			{
				if (!MCostDetail.createShipment(as, line.getAD_Org_ID(), line.getM_Product_ID(), 
						line.getM_AttributeSetInstance_ID(), line.get_ID(), 0, tAmt, tQty, 
						line.getDescription(), false, getTrxName()))
					return "SaveError";
			}
		} 
		else
		{
			if (!MCostDetail.createShipment(as, line.getAD_Org_ID(), line.getM_Product_ID(), 
					line.getM_AttributeSetInstance_ID(), line.get_ID(), 0, tAmt, tQty, 
					line.getDescription(), false, getTrxName()))
				return "SaveError";
		}
		return "";
	}
	
	@Override
	public boolean isDeferPosting() {
		return m_deferPosting;
	}
	

	// sysnova ***************SL-0001****************************
	/** TableName=M_CostElement */
    public static final String Table_Name = "M_CostElement";
	private Collection<MCostElement> getCostElements(Properties ctx)
	{
		return getByCostingMethod(ctx, MCostElement.COSTINGMETHOD_StandardCosting);
	}
	/**
	 * Get All Cost Elements for current AD_Client_ID
	 * @param ctx context
	 * @param trxName transaction
	 * @return array cost elements
	 **/
	public static List<MCostElement> getByCostingMethod (Properties ctx, String CostingMethod)
	{		
		final String whereClause = "AD_Client_ID = ? AND costelementtype=? AND CostingMethod is Null";
		return new Query(ctx, Table_Name, whereClause, null)
					.setOnlyActiveRecords(true)
					.setParameters(Env.getAD_Client_ID(ctx),'M')
					.list();	
	}
	/**
	 * 	Get Current Cost Price for Costing Level
	 *	@param product product
	 *	@param M_ASI_ID costing level asi
	 *	@param Org_ID costing level org
	 *	@param M_CostType_ID cost type
	 *	@param as AcctSchema
	 *	@param costingMethod method
	 *	@param qty quantity
	 *	@param C_OrderLine_ID optional order line
	 *	@param zeroCostsOK zero/no costs are OK
	 *	@param trxName trx
	 *	@return cost price or null
	 */
	public static ArrayList getCurrentCost (MProduct product,int M_ASI_ID,
		MAcctSchema as, int Org_ID, int M_CostType_ID,
		String costingMethod, BigDecimal qty, int C_OrderLine_ID,
		boolean zeroCostsOK, String trxName)
	{
		ArrayList data = new ArrayList();
		
		String CostingLevel = product.getCostingLevel(as);
		if (MAcctSchema.COSTINGLEVEL_BatchLot.equals(CostingLevel))
			Org_ID = 0;
		String sql = "SELECT"
			+ " COALESCE(SUM(c.CurrentCostPrice),0),c.M_CostElement_ID,"		// 1
			+ " ce.CostElementType, ce.CostingMethod,"		// 2,3
			+ " c.Percent,"			// 4,5
			+ " COALESCE(SUM(c.CurrentCostPriceLL),0) "		// 6
			+ " FROM M_Cost c"
			+ " JOIN M_CostElement ce ON (c.M_CostElement_ID=ce.M_CostElement_ID) "
			+ "WHERE c.AD_Client_ID=? AND c.AD_Org_ID=?"		//	#1/2
			+ " AND c.M_Product_ID=?"							//	#3
			+ " AND (c.M_AttributeSetInstance_ID=? OR c.M_AttributeSetInstance_ID=0)"	//	#4
			+ " AND c.M_CostType_ID=? AND c.C_AcctSchema_ID=?"	//	#5/6
			+ " AND (ce.CostingMethod IS NULL) "
			+ " AND ce.isactive='Y' "
			+ "GROUP BY ce.CostElementType, ce.CostingMethod, c.Percent, c.M_CostElement_ID";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql, trxName);
			pstmt.setInt (1, product.getAD_Client_ID());
			pstmt.setInt (2, Org_ID);
			pstmt.setInt (3, product.getM_Product_ID());
			pstmt.setInt (4, M_ASI_ID);
			pstmt.setInt (5, M_CostType_ID);
			pstmt.setInt (6, as.getC_AcctSchema_ID());
			//pstmt.setInt (7, MCostElement_ID);
			rs = pstmt.executeQuery ();
			while (rs.next ())
			{
				BigDecimal currentCostPrice = rs.getBigDecimal(1);
				if (currentCostPrice != null && currentCostPrice.signum() != 0)
				{
					Vector<Object> line = new Vector<Object>();
					line.add(rs.getInt(2));
					line.add(rs.getBigDecimal(1));
					data.add(line);
				}
				
				
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

		
		return data;
	}	//	getCurrentCost
	
	public static int getC_ValidCombination (int costElementData_ID,MAcctSchema as, String trxName )
	{
		int costclearing_acct=0;
		String sql = " SELECT costclearing_acct FROM  M_CostElement_Acct "+
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
				costclearing_acct=rs.getInt(1);
				
				
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

		
		return costclearing_acct;
	}
	public static int getC_ValidCombinationCostVariance (int costElementData_ID,MAcctSchema as, String trxName )
	{
		int CostVariance_acct=0;
		String sql = " SELECT CostVariance_acct FROM  M_CostElement_Acct "+
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
				CostVariance_acct=rs.getInt(1);
				
				
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

		
		return CostVariance_acct;
	}
	
	public static int getC_ValidCombinationCOGSVariance (int costElementData_ID,MAcctSchema as, String trxName )
	{
		int CostVariance_acct=0;
		String sql = " SELECT cogsvar_acct FROM  M_CostElement_Acct "+
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
				CostVariance_acct=rs.getInt(1);
				
				
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

		
		return CostVariance_acct;
	}
	ArrayList VarianceData =new ArrayList();
	public static int getC_ValidCombinationVariance_acct (int costElementData_ID,MAcctSchema as, String trxName )
	{
		int costclearing_acct=0;
		String sql = " SELECT CostVariance_acct FROM  M_CostElement_Acct "+
				" WHERE  isactive='Y' AND m_costelementvariance_id=? AND C_AcctSchema_ID=? AND AD_Client_ID=? and isActive='Y'";
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
				costclearing_acct=rs.getInt(1);
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

		
		return costclearing_acct;
	}

	public static int getcostElementReverse_ID (int costElementData_ID,MAcctSchema as, String trxName )
	{
		int m_costElementvariance_ID=0;
		String sql = " SELECT m_costElement_ID FROM  M_CostElement_Acct"
				+ " WHERE  m_costElementvariance_ID=?"
				+ " AND AD_Client_ID=? and isActive='Y'";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement (sql, trxName);
			pstmt.setInt (1, costElementData_ID);
			pstmt.setInt (2, as.getAD_Client_ID());
			rs = pstmt.executeQuery ();
			if(rs.next ())
			{
				m_costElementvariance_ID=rs.getInt(1);
				
				
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

		
		return m_costElementvariance_ID;
	}
	// sysnova ****END********SL-0001******END*********
}   //  Doc_InOut
