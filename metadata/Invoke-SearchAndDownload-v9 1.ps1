######################################################################################
#
# GenAI Search Content Manager via Service API
#
# Change Log
#   v7 -- 1. Add CustomerUCN separators to be " OR " and not ","
#         2. Ensure only download document
#         3. Have option to download metadata only instead metadata+document
#   v8 -- 1. Add StartDate and EndDate parameters to allow for searching for 
#            changes in a specific date range
#         2. Added example invocations in the script parameters section
#         3. Renamed parameter "SearchAll" to "FullExtract" to better 
#            reflect the behavior
#         4. Changed parameter types to bool (from string) for FullExtract,
#            Download, DownloadAll, and BypassConfirm to resolve issues
#            occurring when evaluating the parameters at runtime
#         5. Added transcript logging
#         6. Moved confirmation logic to a function and moved glboal variables
#            to top of the script for easier access
#         7. Added launch transcript parameter and logic to check
#            the parameter value.  When $true, the script will launch
#            the transcript file in the default application at end of script
#         8. Moved launching of batch folder in explorer into the download
#            function to ensure it is only launched when downloads are performed.
#            This resolves the scenario of a search failing, resulting in no
#            batch folder being created, and an error occurring
#   v9 -- 1. Fixed search method for date last updated subquery
#         2. Moved valuesArray and valueFilter to be global variables to prevent scope conflict
#         3. Joined values in array with comma before injecting into search string
#         4. Added logic to test for the batch path directory before launching explorer,
#            resolving the error thrown after a search fails due to a timeout
######################################################################################
#
# Script Parameters
#
#
# Validate, verify, and notify the user of the impact of script parameters. Exit the
# script if any of the parameters is invalid.  Prompt the user for confirmation
# before continuing or terminate.
#
# Example Invocations:
#  - Full Extract, download nothing, and default Start/End dates
#    .\Invoke-SearchAndDownload-v9-23.ps1 -FullExtract $true -Download $false -BypassConfirm $true
#  - Full Extract, download all, and use default Start/End dates
#    .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $true -Download $false -BypassConfirm $true 
#  - Full Extract, Search record uris, Download nothing, and specific Start and End Dates
#    .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $true -Download $false -RecordUris "123456,789012" -BypassConfirm $true -StartDate "2021-01-01 00:01 AM" -EndDate "2021-01-31 23:59 PM"
#  - Full Extract, Search record uris, Download all, and specific Start and End Dates
#    .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $false -Download $true -RecordUris "123456,789012" -DownloadAll $true -BypassConfirm $false -StartDate "2021-01-01 00:01 AM" -EndDate "2021-01-31 23:59 PM"
#    .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $false -Download $true -RecordUris "1079051,1078803" -DownloadAll $true -BypassConfirm $false -StartDate "2025-01-01 00:01 AM" -EndDate "2025-09-30 23:59 PM"
#    .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $false -Download $false -DownloadAll $false -BypassConfirm $false -StartDate "2025-01-01 00:01 AM" -EndDate "2025-02-28 23:59 PM"
#    .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $false -Download $true -CustomerUCNS "01018453,123456" -BypassConfirm $true -StartDate "2025-02-01 00:01 AM" -EndDate "2025-04-28 23:59 PM"
######################################################################################
param(
    [parameter(Mandatory = $true, HelpMessage = "When TRUE the script will search for all contract records. When FALSE the search will return records newly registered or updated.")][bool]$FullExtract = $false,
    [parameter(Mandatory = $true, HelpMessage = "When TRUE the script will downlooad records having electronic attachments")][bool]$Download = $true,
    [parameter(Mandatory = $false, HelpMessage = "When provided with a comma-separated list of record URIs the search will be limited to just those records. When value set and -FullExtract is FALSE then the URIs are only returned if changed in the past few days. ")][string]$RecordUris,
    [parameter(Mandatory = $false, HelpMessage = "When provided with a comma-separated list of record numbers the search will be limited to just those records. When value set and -FullExtract is FALSE then the numbers are only returned if changed in the past few days. ")][string]$RecordNumbers,
    [parameter(Mandatory = $false, HelpMessage = "When provided with a comma-separated list of Customer UCNs the search will be limited to just those records. When value set and -FullExtract is FALSE then the UCNs are only returned if changed in the past few days. ")][string]$CustomerUCNs,
    [parameter(Mandatory = $false, HelpMessage = "When set to TRUE the script will download a metadata field and all documents.  When False the script will download a metadat document and no more than 5 documents per contract folder.")][bool]$DownloadAll = $true,
    [parameter(Mandatory = $false, HelpMessage = "When set to TRUE the script will not pause to ask for confirmation. Use this when configuring nightly jobs running in unattended mode.")][bool]$BypassConfirm = $false,
    [parameter(Mandatory = $false, HelpMessage = "Specifies the desired start date to be used when searching for contracts.  Defaults to current date & time minus 24 hours.")][string]$StartDate,
    [parameter(Mandatory = $false, HelpMessage = "Specifies the desired end date to be used when searching for contracts.  Defalts to current date & time.")][string]$EndDate,
    [parameter(Mandatory = $false, HelpMessage = "When set to TRUE the script launch the transcript in default application at the end of execution.")][bool]$ViewTranscript = $false
)
# .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $true -Download $false -DownloadAll $false -BypassConfirm $false -StartDate "2020-01-01 00:01 AM" -EndDate "2025-02-28 23:59 PM"
# .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $true -Download $false -DownloadAll $false -BypassConfirm $false -StartDate "1900-01-01 00:01 AM" -EndDate "2050-02-28 23:59 PM"
# .\Invoke-SearchAndDownload-v9.ps1 -FullExtract $true -Download $true -DownloadAll $true -BypassConfirm $false -StartDate "1900-01-01 00:01 AM" -EndDate "2050-02-28 23:59 PM"
$version = "v9"
$tpath      = $env:TEMP
$username   = $env:USERNAME
$hostname   = hostname
$datetime   = Get-Date -f 'yyyyMMddHHmmss'
$filename   = "Transcript-${username}-${hostname}-${version}-${datetime}.txt"
$Transcript = Join-Path -Path $tpath -ChildPath $filename
Start-Transcript -Path $Transcript | Out-Null

#region Environment Configuration
[DateTime]$scriptExecutionDateTime = Get-Date
$scriptExecutionDateTimeFormatted = Get-Date $scriptExecutionDateTime -format 'G'
$InformationPreference = 'continue'
$DebugPreference = 'continue'
$RouteUrl = "http://awsdrbnvaw0003/CMServiceAPI/Record"  # URL defines the route we can invoke to search for and download metadata & documents
$webClient = New-Object System.Net.WebClient  # Construct a web client object which will be used to invoke the AP
#$webClient.UseDefaultCredentials = $true       # Unnecessary if ServiceAPI is configured for anonymous guest login, no credentials required
$formatResultsParameter = "format=json"  # Query string parameter used by HTTP GET to records endpoint.  Format=Json forces usage of API feature (as opposed to HTML/XML).
$pageSize = 3000  # Page Size prevents us from having to download multiple sets of results.
$paginationParameter = "pageSize=$($pageSize)"
$contentTypeHeader = "application/json" # Add the content type to the header of any POST requests (right now everything is a GET)
$propertiesParameter = "RecordRelatedRecord,RecordAttachedKeywords,RecordKeywords"  # Add fields to retrieve them even if not in the property set.  Included to enable ad-hoc changes in future.
$propertySetsParameter = "GenAI"  # Passed into the ServiceAPI defining which fields we want back in result
$rootDownloadFolder = "c:\temp\genai"  # Location where files will be stored during download
$batchId = ("$($env:USERNAME)".Replace(" ", "") + "_$(Get-Date  -Format "yyyyMMddHHmm")")  # Generates a unique string based current user name and the date time (down to minute)
$batchPath = Join-Path -Path $rootDownloadFolder -ChildPath $batchId # Root folders within which the metadata file will be placed
$attachmentsPath = Join-Path -Path $batchPath -ChildPath "docs"  # Subfolder of root where downloaded documents will be placed
$metadataPath = Join-Path -Path $batchPath -ChildPath "metadata.json"  # Path to the  metadata extract file, including file name
$recordTypeFilter = " type:[name:contract*]"
$global:valuesArray = @()
$global:valueFilter = ""
$global:valueSearchString = ""
#endregion

#region Functions
######################################################################################
#
# ConfirmOrExit
#
# Displays the parameters detected and expected behavior.  Prompts the user 
# to confirm both and exits script if not confirmed.
#
######################################################################################
function ConfirmOrExit {
    Clear-Host
    Write-Host "------------------------------------------------"
    Write-Host "       Executing Download Contracts Script"
    Write-Host "------------------------------------------------"
    Write-Host " * Execution Time: $($scriptExecutionDateTimeFormatted)"
    Write-Host " * Transcript: $($Transcript)"
    #####################################################
    Write-Host "Global Configuration"
    #####################################################
    Write-Host " - ServiceAPI URL: $($RouteUrl)"
    Write-Host " - Format Results: $($formatResultsParameter)"
    Write-Host " - Page Size: $($pageSize)"
    Write-Host " - Page Size Param: $($paginationParameter)"
    Write-Host " - Content Type (POST): $($contentTypeHeader)"
    Write-Host " - Extra Properties Requested: $($propertiesParameter)"
    Write-Host " - Property Sets Requested: $($propertySetsParameter)"
    Write-Host " - Download Folder: $($rootDownloadFolder)"
    Write-Host " - Batch ID: $($batchId)"
    Write-Host " - Batch Path: $($batchPath)"
    Write-Host " - Attachments Path: $($attachmentsPath)"
    Write-Host " - Metadata Path: $($metadataPath)"
    Write-Host " - Record Type Filter: $($recordTypeFilter)"
    Write-Host ""    
    Write-Host "Runtime Parameters ... Please confirm!"
    # Confirm intention that all contracts be evaluated
    if ( $true -eq $FullExtract ) {
        Write-Host " * Execution Mode: Full Extract"
    } else {
        Write-Host " * Execution Mode: Incremental Update"
    }
    # Confirm desired date range
    $global:changeScopePreviousdays = 1  # Define how far back in time changes are searched for when finding change
    if ( [string]::IsNullOrWhiteSpace($EndDate) -eq $true -and [string]::IsNullOrWhiteSpace($StartDate) -eq $true ) {
        Write-Host " * Searching default end date and default start date"
        $EndDate = "$(Get-Date $scriptExecutionDateTime -format 'G')"
        $StartDate = "$(Get-Date (Get-Date $scriptExecutionDateTime).AddDays(-$($changeScopePreviousdays)) -format 'G')"
    } elseif ([string]::IsNullOrWhiteSpace($EndDate) -eq $false -and [string]::IsNullOrWhiteSpace($StartDate) -eq $true ) {
        Write-Host " * Searching custom end date and default start date"
        $StartDate = "$(Get-Date (Get-Date $EndDate).AddDays(-$($changeScopePreviousdays)) -format 'G')"
    } elseif ([string]::IsNullOrWhiteSpace($EndDate) -eq $true -and [string]::IsNullOrWhiteSpace($StartDate) -eq $false ) {
        Write-Host " * Searching default end date and custom start date"
        $EndDate = "$(Get-Date $scriptExecutionDateTime -format 'G')"
    } else {
        Write-Host " * Searching custom end date and custom start date"
    }
    if ( -not ($StartDate -like "*:*" -or $StartDate -like "*:*") ) {
        Write-Host "   - Detected no time in Start Date! Will default to 00:00 AM"
        $StartDate = Get-Date $StartDate -format 'G'
    }
    Write-Host "   - Start Date: $($StartDate)"
    if ( -not ($EndDate -like "*:*" -or $EndDate -like "*:*") ) {
        Write-Host "   - Detected no time in End Date! Will default to 00:00 AM"
        $EndDate = Get-Date $EndDate -format 'G'
    }
    Write-Host "   - End Date: $($EndDate)"
    
    # Confirm that when searching for changes and supplying record URIs
    if ( $false -eq $FullExtract -and $true -ne [string]::IsNullOrEmpty($recordUris) ) {
        Write-Host " * Record URIs supplied whilst searching through changes only"
    }
    elseif ($true -eq $FullExtract -and $true -ne [string]::IsNullOrEmpty($recordUris) ) {
        Write-Host " * Record URIs supplied whilst searching through all"
    }
    
    if (  $false -eq [string]::IsNullOrEmpty($recordUris) -and $false -eq [string]::IsNullOrEmpty($RecordNumbers) ) {
        Write-Error " * Must not provide both URIs and Numbers parameters, quiting"
        return $false
    }
    # Confirm searching by values


    if (  $false -eq [string]::IsNullOrEmpty($CustomerUCNs) ) {
        $global:valuesArray = $CustomerUCNs.Split(' OR ')
        $global:valueFilter = "CustomerUCN"
        Write-Host " * Imported UCNs from script parameter"
    } elseif ( $false -eq [string]::IsNullOrEmpty($RecordUris) ) {
        $global:valuesArray = $RecordUris.Split(',')
        $global:valueFilter = "URI"
        Write-Host " * Imported Record Uris from script parameter"
    } elseif ( $false -eq [string]::IsNullOrEmpty($RecordNumbers) ) {
        $global:valuesArray = $RecordNumbers.Split(',')
        $global:valueFilter = "Number"
        Write-Host " * Imported Record Numbers from script parameter"
    }
    if ( 0 -eq $global:valuesArray.Length ) {
        Write-Host " * No specific search values detected (URI, Number, UCN)"
    } elseif ( 1 -eq $global:valuesArray.Length ) {
        Write-Host " * Only one search value was detected "
    } else {
        Write-Host " * Searching for $($global:valuesArray.Length) contracts by $($global:valueFilter)"
    }
    if ( $global:valuesArray.Length -gt 0 ) {
        $numbers = $global:valuesArray -join ","
        $global:valueSearchString = "$($global:valueFilter):$($numbers)"
        Write-Host " - Value Search String: $($global:valueSearchString)"
    }
    # Confirm downloading
    if ( $download -eq $true -and $downloadAll -eq $true ) {
        Write-Host " * Documents will be downloaded (no limits)"
    } elseif ($Download -eq $true -and $DownloadAll -eq $false ) {
        Write-Host " * Documents will be downloaded (limit of 5 per contract)"
    } else {
        Write-Host " * Documents will NOT be downloaded"
    }
    # Prompt for confirmation if preferece set
    if ( $false -eq $BypassConfirm ) {
        Write-Host ""
        $confirmation = Read-Host "Do you want to continue? [y to continue] "
        if ($confirmation -ne 'y') {
            Write-Host "User chose to not continue.  Exiting script"
            return $false
        } else {
            return $true
        }
    }
    elseif ( $true -eq $BypassConfirm ) {
        Write-Host "Bypassing confirmation due to script parameter value"
        return $true
    }
    return $false
}



######################################################################################
#
# Get Search Results
#
#
# Formulate the search string to be passed into the ServiceAPI, push it into the
# URL along with pagination, property sets, properties, and JSON format parameters.
# Only return the response payload if HTTP Status Code 200 detected in response.  For
# all other status codes print to error sink.
#
######################################################################################
function Get-SearchResults {
    param([string] $Query, $PageIndex = -1)
    $result = $null
    try {
        # Bug in ServiceAPI requires us to replace the tilde character
        # with a question mark only when searching record number
        $finalQuery = ([uri]::EscapeUriString($Query))
        if ( "Number" -eq $valueFilter ) {
            $finalQuery = $finalQuery.replace('~', '?')
        }
        Write-Debug "Escaped Query: $($finalQuery)"
        $searchUri = ("$($RouteUrl)?q=$($finalQuery)&propertySets=$($propertySetsParameter)&properties=$($propertiesParameter)&$($formatResultsParameter)&$($paginationParameter)")
        Write-Debug "Search URI: $($searchUri)"
        Write-Host "Invoking Search via ServiceAPI: $($finalQuery)"
        $response = $webClient.DownloadString($searchUri)
       
        if ( $response.StatusCode -eq 200 ) {
            Write-Error "Unexpected HTTP Status Code $($response.StatusCode)"
        } elseif ([string]::IsNullOrWhiteSpace($response) -eq $false) {
            # Bug when parsing UTC date times, remove the timeszone resolves
            $response = $response.Replace('.0000000Z"', '"')
            Write-Debug "Received Bytes: $($response.Length)"
            $result = $response | ConvertFrom-Json
            Write-Debug "Result: $($result)"
            Write-Information "Search received a response"
        } else {
            Write-Information "Search failed to return results"
        }
    } catch [Exception] {
        $exceptionMessage = "Get-SearchResults failed: $($_.Message)"
        Write-Host "Exception: $($exceptionMessage)"
        Write-Host $_
    }
    Write-Debug "Search result: $($result)"
    return $result
}

######################################################################################
#
# Get Record Download
#
#
# Formulate the URL to the download and the path to local disk (or S3 bucket) where
# the HTTP Response will be streamed & written to disk. Only return the response
# payload if HTTP Status Code 200 detected in response.  For all other status codes
# print to error sink.
#
######################################################################################
function Get-RecordDownload {
    param([long]$RecordUri, [string]$extension)
    $downloadUrl = ""
    $LocalFile = ""
    $result = $null
    try {
        $downloadUrl = ("$RouteUrl/$($RecordUri)/File/document")
        # fix extension if blank.  Requirements state all documents should be PDF so we'll
        if ( [string]::IsNullOrWhiteSpace($extension) ) { $extension = ".pdf" }
        # formulate path where download will be placed
        $LocalFile = "$($attachmentsPath)\$($RecordUri).$($extension)"
        Write-Debug "Downloading record from: $($downloadUrl)"
        Write-Debug "Downloading record to: $($LocalFile)"
        # reuse the web client instantiated in the global space
        $webClient.DownloadFile($downloadUrl, $LocalFile)
        Write-Information "Downloaded Uri $($RecordUri) to: $($LocalFile)"
        $result = $LocalFile
    }
    catch [Exception] {
        $exceptionMessage = "Get-RecordDownload failed: $($_.Message)"
        Write-Error -Exception $_ -Message "Attempted download of URI $($RecordUri) from URL ($($downloadUrl)) to file ($($LocalFile))"
        Write-Host "Exception: $($exceptionMessage)"
        Write-Host $_
    }
    Write-Debug "Get-Download result: $($result)"
    return $result
}

######################################################################################
#
# Invoke-SearchAndDownload
#
#
# Use the globally scoped objects and script parameters to implement the business
# logic resulting in the metadata file being generated after executing the search
# via the ServiceAPI.  Then download each file locally.
#
######################################################################################

function Invoke-SearchAndDownload {
    #####################################################
    Write-Host "Step 2 - Search for contracts"
    #####################################################
    # Formulate the query search string
    $searchString = ""
    # Add either all or
    if ( $true -eq $FullExtract  ) {
        $searchString += "all "
    }
    else {
        # previous logic was to search for changes in the past two days
        #$searchString += "(registeredOn:previous $($changeScopePreviousdays) days OR updated:previous $($changeScopePreviousdays) days)"
        # new logic uses script parameters StartDate and EndDate
        $searchString += "((registeredOn:$($StartDate) TO $($EndDate)) OR (updated:$($StartDate) TO $($EndDate)))"
    }
    # add any value filters to the search string
    if ( $false -eq [string]::IsNullOrWhiteSpace($global:valueSearchString) ) {
        $searchString += " AND ($($global:valueSearchString) OR container:[$($global:valueSearchString)]) "
    }
    $searchString += " AND $($recordTypeFilter)"
    # add in the filters to contract types only
    Write-Debug "Search to execute: $($searchString)"
    $searchResults = Get-SearchResults -Query $searchString
    # validate the response
    if ( $null -eq $searchResults ) {
        Write-Host " * Search failed"
        return
    }
    else {
        Write-Host "Search successful"
    }
   
    # when successful search ensure the batch folders have been create
    if ( $false -eq (Test-Path -Path $batchPath) ) {
        Write-Host " * Creating Directory for Results"
        New-Item -ItemType Directory -Path $batchPath | Out-Null
    }
    if ( $false -eq (Test-Path -Path $attachmentsPath) ) {
        Write-Host " * Creating Directory for Documents"
        New-Item -ItemType Directory -Path $attachmentsPath | Out-Null
    }
    #####################################################
    Write-Host "Step 3 - Save metadata file"
    #####################################################
    $searchResults | ConvertTo-Json -Depth 8 | Set-Content $metadataPath
    #$results = Get-Content -Path $metadataPath  | ConvertFrom-Json $metadataJsonContent
    if ( $download -eq $false ) {
        ii $batchPath
        Write-Host "Skipping downloads"
        return
    }
    #####################################################
    Write-Host "Step 4 - Downloading $($searchResults.TotalResults) documents"
    #####################################################
    $downloadCount = 0
    foreach ( $Record in $searchResults.Results ) {
        #Write-Host $Record.Fields.CustomerUCN
        if ( [string]::IsNullOrWhiteSpace($Record.RecordExtension.Value) -eq $false -and $Record.RecordExtension.Value -eq "pdf" -or $Record.RecordExtension.Value -eq "zip") {
            Write-Host  "       * Downloading Record: $($Record.RecordNumber.Value)"
            Get-RecordDownload -recordUri $Record.Uri -extension $Record.RecordExtension.Value
            $downloadCount = $downloadCount + 1
        }
        if ( $false -eq $downloadAll -and $downloadCount -gt 5 ) {
            break
        }
    }
    Write-Host "Downloaded a total of $($downloadCount) documents"
    Write-Host "Contract export complete"
    ii $batchPath
}
#endregion
 
if ( ConfirmOrExit -eq $true ) {
    $start_time = Get-Date
    Invoke-SearchAndDownload
    # Launch Explorer to view the results
    if ( (Test-Path $batchPath) ) {
        ii $batchPath
    }
    $end_time = Get-Date
    $duration = $end_time - $start_time
    Write-Host "Total execution time: $duration"
}
 
Write-Host "------------------------------------------------"
Write-Host "       Script Execution Completed"
Write-Host "------------------------------------------------"
Stop-Transcript | Out-Null
if ( $ViewTranscript -eq $true ) {
    Write-Host "Launching Transcript for Viewing"
    ii $Transcript
}