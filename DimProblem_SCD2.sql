


CREATE PROCEDURE [wpss].[sp_SSIS_RawTo_DimProblem]
	@p_ExecutionBreakpointId BIGINT
AS
BEGIN
	/*
		-----------------------------------------------------------------------------------------------
		Description: Transfer the data from raw into DimProblem 
 
		Parameter Mapping:	@p_ExecutionBreakpointId BIGINT, - wpss.tbl_ExecutionAuditBreakpoint.Id
		-------------------------------------------------------------------------------------------------
		Version  Who    Date         Change
        1.0      SD     13/05/2024   Initial Script 
	*/	

	DECLARE @auditNote VARCHAR(MAX)
	DECLARE @processname VARCHAR(200)
	DECLARE @executionAuditId INT
	DECLARE @executionAuditDetailId BIGINT

	SET NOCOUNT ON;

	BEGIN TRY

		BEGIN TRANSACTION TRANSACTION_UPDATEDIMPROBLEM;

		SET @processname = OBJECT_NAME(@@PROCID)
	
		SELECT @executionAuditId = ExecutionAuditId
		FROM wpss.tbl_ExecutionAuditBreakpoint a
		WHERE a.Id = @p_ExecutionBreakpointId

		SET @auditNote = 'Executing MERGE process  ' + @processname  
		EXEC wpss.sp_SSIS_AddAuditDetail @executionAuditId, @p_ExecutionBreakPointId, @processname, @auditNote, 'Raw, Merge, DimProblem', @executionAuditDetailId 

		/* Merge the data from the raw source */
		INSERT wpsDW.DimProblem([problemSeq] ,[Code] ,[Name], [problemDescription] , [problemCode] ,
		[priorityDescription] , [prorityCode] , [categoryDescription] , [categoryCode] , [labourType] , [labourCode] ,		
		insertedAuditBreakpointId, updatedAuditBreakpointId)
			SELECT [problemSeq] ,[Code] ,[Name], [problemDescription] , [problemCode] ,	[priorityDescription] , [prorityCode] , 
			[categoryDescription] , [categoryCode] , [labourType] , [labourCode],
			@p_ExecutionBreakpointId, @p_ExecutionBreakpointId                                    
			FROM  (
				
					MERGE wpsDW.DimProblem AS Target
					USING 
					(							
						SELECT  [problemSeq] ,[Code] ,[Name], [problemDescription] , [problemCode] ,
								[priorityDescription] , [prorityCode] , [categoryDescription] , [categoryCode] , [labourType] , [labourCode]                       
						FROM    wpss.tbl_raw_evo_dbo_problem
					) AS Source
					ON (Target.[problemSeq] = Source.[problemSeq])    
					-------------------------------                       
					WHEN MATCHED AND 
					(
						Target.[code] <> Source.[code] 
						OR Target.[name] <> Source.[name]
						OR Target.[problemDescription] <> Source.[problemDescription]
						OR Target.[problemCode] <> Source.[problemCode]
						OR Target.[priorityDescription] <> Source.[priorityDescription]
						OR Target.[prorityCode] <> Source.[prorityCode]
						OR Target.[categoryDescription] <> Source.[categoryDescription]
						OR Target.[categoryCode] <> Source.[categoryCode]
						OR Target.[labourType] <> Source.[labourType]
						OR Target.[labourCode] <> Source.[labourCode]
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
							 [problemSeq]
							, [Code]
							, [Name]
							, [problemDescription]
							, [problemCode]
							, [priorityDescription]
							, [prorityCode]
							, [categoryDescription]
							, [categoryCode]
							, [labourType]
							, [labourCode]                  
							,insertedAuditBreakpointId
							,updatedAuditBreakpointId 
						)
						VALUES (      
							 Source.[problemSeq]
							, Source.[Code]
							, Source.[Name]
							, Source.[problemDescription]
							, Source.[problemCode]
							, Source.[priorityDescription]
							, Source.[prorityCode]
							, Source.[categoryDescription]
							, Source.[categoryCode]
							, Source.[labourType]
							, Source.[labourCode]   
                            , @p_executionBreakpointId
							, @p_executionBreakpointId
						)
                    -------------------------------
                    WHEN NOT MATCHED BY SOURCE 
                        THEN 
                        UPDATE SET 				
							Target.[endDataDate] = getdate(),
							Target.UpdatedAuditBreakpointId = @p_executionBreakpointId	
            
                    -------------------------------
                    OUTPUT $Action, Source.*
                    ) AS i([Action],[problemSeq] ,[Code] ,[Name], [problemDescription] , [problemCode] ,
		[priorityDescription] , [prorityCode] , [categoryDescription] , [categoryCode] , [labourType] , [labourCode])
		WHERE [Action] = 'UPDATE'

		COMMIT TRANSACTION TRANSACTION_UPDATEDIMPROBLEM;

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