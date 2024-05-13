
--IF(SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'wpss' AND ROUTINE_NAME = 'sp_SSIS_RawTo_DimPriority' ) > 0
--BEGIN	
--	DROP PROCEDURE [wpss].[sp_SSIS_RawTo_DimPriority];
--END
--GO

CREATE PROCEDURE [wpss].[sp_SSIS_RawTo_DimSupplier]
	@p_ExecutionBreakpointId BIGINT
AS
BEGIN
	/*
		-----------------------------------------------------------------------------------------------
		Description: Transfer the data from raw into DimSupplier 
 
		Parameter Mapping:	@p_ExecutionBreakpointId BIGINT, - wpss.tbl_ExecutionAuditBreakpoint.Id
		-------------------------------------------------------------------------------------------------
		Version  Who    Date         Change
        1.0      SD     09/05/2024   Initial Script 
	*/	

	DECLARE @auditNote VARCHAR(MAX)
	DECLARE @processname VARCHAR(200)
	DECLARE @executionAuditId INT
	DECLARE @executionAuditDetailId BIGINT

	SET NOCOUNT ON;

	BEGIN TRY

		BEGIN TRANSACTION TRANSACTION_UPDATEDIMSUPPLIER;

		SET @processname = OBJECT_NAME(@@PROCID)
	
		SELECT @executionAuditId = ExecutionAuditId
		FROM wpss.tbl_ExecutionAuditBreakpoint a
		WHERE a.Id = @p_ExecutionBreakpointId

		SET @auditNote = 'Executing MERGE process  ' + @processname  
		EXEC wpss.sp_SSIS_AddAuditDetail @executionAuditId, @p_ExecutionBreakPointId, @processname, @auditNote, 'Raw, Merge, DimPriority', @executionAuditDetailId 

		/* Merge the data from the raw source */
		INSERT wpsDW.DimSupplier([supplierSeq],[code],[name], insertedAuditBreakpointId, updatedAuditBreakpointId)
			SELECT [SUP_SEQ],[SUP_CODE],[SUP_NAME],@p_ExecutionBreakpointId, @p_ExecutionBreakpointId                                    
			FROM  (
				
					MERGE wpsDW.DimSupplier AS Target
					USING 
					(
						SELECT   SUP_SEQ  ,SUP_CODE, SUP_NAME                       
						FROM    wpss.tbl_raw_evo_dbo_fsupply
					) AS Source
					ON (Target.supplierSeq = Source.SUP_SEQ)    
					-------------------------------                       
					WHEN MATCHED AND 
					(
						Target.[code] <> Source.[SUP_CODE] OR Target.[name] <> Source.[SUP_NAME]
					)  
					THEN
						UPDATE SET 
							Target.[endDataDate] = getdate(),
							Target.UpdatedAuditBreakpointId = @p_executionBreakpointId
					------------------------------
					WHEN NOT MATCHED BY TARGET 
					THEN
						INSERT 
						( 
							 [supplierSeq]                        
							,[code]
							,[name]                    
							,insertedAuditBreakpointId
							,updatedAuditBreakpointId 
						)
						VALUES (      
                                Source.[SUP_SEQ], 
                                Source.[SUP_CODE],
                                Source.[SUP_NAME], 
                                @p_executionBreakpointId,
								@p_executionBreakpointId
						)
                    -------------------------------
                    WHEN NOT MATCHED BY SOURCE 
                        THEN 
                        UPDATE SET 				
							Target.[endDataDate] = getdate(),
							Target.UpdatedAuditBreakpointId = @p_executionBreakpointId	
            
                    -------------------------------
                    OUTPUT $Action, Source.*
                    ) AS i([Action],SUP_SEQ  ,SUP_CODE, SUP_NAME)
		WHERE [Action] = 'UPDATE'

		COMMIT TRANSACTION TRANSACTION_UPDATEDIMSUPPLIER;

	END TRY
	BEGIN CATCH
				
		DECLARE @errorMessage NVARCHAR(4000),
				 @errorSeverity INT,
				 @ErrorState INT,
				 @ErrorCode INT;

		SELECT
   			@errorMessage = ERROR_MESSAGE(),
			@errorSeverity = ERROR_SEVERITY(),
			@errorState = ERROR_STATE(),
			@errorCode = ERROR_NUMBER();
			
		EXEC wpss.sp_SSIS_RaiseError @executionAuditId, @p_ExecutionBreakpointId, @errorCode, @errorMessage, @processName

		ROLLBACK TRANSACTION TRANSACTION_UPDATEDIMSUPPLIER;
		
		THROW
		
	END CATCH
END