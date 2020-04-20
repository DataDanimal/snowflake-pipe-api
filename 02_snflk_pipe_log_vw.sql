/*************************************
View to aggregate log, deploy after client first runs
*************************************/
create or replace view pipe_log_vw as
select
         l.pipe_log_seq
      ,  pipe.seq results_seq
      ,  pipe.index results_index
      ,  l.pipe_log_msg:Pipe::string pipe
      ,  l.pipe_log_msg:startTimeInclusive::timestamp starttime
      ,  l.pipe_log_msg:endTimeExclusive::timestamp endtime
      ,  TIMESTAMPDIFF('seconds', starttime, endtime)  pipe_duration
      ,  pipe_duration / 60 pipe_duration_mins
      ,  l.pipe_log_msg:completeResult::boolean completeResult
      ,  l.pipe_log_msg:rangeStartTime::timestamp rangeStartTime
      ,  l.pipe_log_msg:rangeEndtime::timestamp rangeEndtime
      ,  pipe.value:Complete::boolean Complete
      ,  pipe.value:ErrorsLimit::bigint ErrorsLimit
      ,  pipe.value:ErrorsSeen::bigint ErrorsSeen
      ,  pipe.value:FileSize::float FileSize
      ,  FileSize / 1024 FileSizeKB
      ,  FileSizeKB / 1024 FileSizeMB
      ,  FileSizeMB / 1024 FileSizeGB
      ,  pipe.value:LastInsertTime::timestamp LastInsertTime
      ,  pipe.value:TimeReceived::timestamp TimeReceived
      ,  TIMESTAMPDIFF('seconds', TimeReceived, LastInsertTime)  duration
      ,  duration / 60 duration_mins
      ,  pipe.value:Path::string Path
      ,  pipe.value:RowsInserted::bigint RowsInserted
      ,  pipe.value:RowsParsed::bigint RowsParsed
      ,  pipe.value:StageLocation::string StageLocation
      ,  pipe.value:Status::string Status
      ,  RowsInserted / duration RowsPerSec
      ,  FileSizeMB / duration MBPerSec
     ,  FileSizeGB / duration GBPerSec
      ,  FileSizeKB / duration KBPerSec
      ,  FileSize / duration BytesPerSec
      , case when lower(pipe.value:Path::string) like '%.json%'
             then 'JSON'
             when lower(pipe.value:Path::string) like '%.tsv%'
             then 'TSV'
             when lower(pipe.value:Path::string) like '%.csv%'
             then 'CSV'
             else 'UNK'
         end fileFormat
     /* , res.Complete
      ,  res.value:Complete::boolean complete*/
      //, pipe.*
      ,l.pipe_log_msg
  from PIPE_API_LOG l
    ,lateral flatten (input => l.pipe_log_msg:result) pipe
   where fileFormat in ('JSON', 'TSV', 'CSV')
    ;

create or replace view pipe_log_agg_vw as
select v.pipe_log_seq
      ,v.pipe
      ,v.fileFormat
      , v.pipe_duration
      , v.pipe_duration_mins
      , v.starttime
      , v.endtime
      , v.rangeStartTime
      , v.rangeEndTime
      , count(1) Files
      , sum(v.Complete::int) FilesCompleted
      , Files - FilesCompleted FilesRemaining
      //, v.ErrorsLimit
      , avg(v.duration) AverageDuration
      , min(v.duration) MinDuration
      , max(v.duration) MaxDuration
      , AverageDuration / 60 AverageDuration_Mins
      , MinDuration / 60 MinDuration_Mins
      , MaxDuration / 60 MaxDuration_Mins
      , sum(v.FileSizeGB) TotalFileGB
      , avg(v.FileSizeGB) AverageFileGB
      , min(v.FileSizeGB) MinFileGB
      , max(v.FileSizeGB) MaxFileGB
      , sum(v.FileSizeMB) TotalFileMB
      , avg(v.FileSizeMB) AverageFileMB
      , min(v.FileSizeMB) MinFileMB
      , max(v.FileSizeMB) MaxFileMB
      , sum(v.FileSizeKB) TotalFileKB
      , avg(v.FileSizeKB) AverageFileKB
      , min(v.FileSizeKB) MinFileKB
      , max(v.FileSizeKB) MaxFileKB
      , sum(v.FileSize) TotalFileSize
      , avg(v.FileSize) AverageFileSize
      , min(v.FileSize) MinFileSize
      , max(v.FileSize) MaxFileSize
      , sum(v.RowsInserted) TotalRowsInserted
      , avg(v.RowsInserted) AverageRowsInserted
      , min(v.RowsInserted) MinRowsInserted
      , max(v.RowsInserted) MaxRowsInserted
      , sum(v.ErrorsSeen) TotalErrorsSeen
      , avg(v.ErrorsSeen) AverageErrorsSeen
      , min(v.ErrorsSeen) MinErrorsSeen
      , max(v.ErrorsSeen) MaxErrorsSeen
      ,  TotalRowsInserted / pipe_duration RowsPerSec
      ,  TotalFileMB / pipe_duration MBPerSec
      ,  TotalFileGB / pipe_duration GBPerSec
      ,  TotalFileKB / pipe_duration KBPerSec
      ,  TotalFileSize / pipe_duration BytesPerSec
  from pipe_log_vw v
  group by 1,2,3,4,5,6,7, 8, 9;

  desc view pipe_log_agg_vw;