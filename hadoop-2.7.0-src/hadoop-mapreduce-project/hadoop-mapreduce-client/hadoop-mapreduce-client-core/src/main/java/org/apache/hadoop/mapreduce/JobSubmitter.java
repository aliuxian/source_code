/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.QueueACL;

import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Charsets;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobSubmitter {
  protected static final Log LOG = LogFactory.getLog(JobSubmitter.class);
  private static final String SHUFFLE_KEYGEN_ALGORITHM = "HmacSHA1";
  private static final int SHUFFLE_KEY_LENGTH = 64;
  private FileSystem jtFs;
  private ClientProtocol submitClient;
  private String submitHostName;
  private String submitHostAddress;
  
  JobSubmitter(FileSystem submitFs, ClientProtocol submitClient) 
  throws IOException {
    this.submitClient = submitClient;
    this.jtFs = submitFs;
  }
  
  /**
   * configure the jobconf of the user with the command line options of 
   * -libjars, -files, -archives.
   * @param job
   * @throws IOException
   */
  private void copyAndConfigureFiles(Job job, Path jobSubmitDir) 
  throws IOException {
    JobResourceUploader rUploader = new JobResourceUploader(jtFs);
    rUploader.uploadFiles(job, jobSubmitDir);

    // Get the working directory. If not set, sets it to filesystem working dir
    // This code has been added so that working directory reset before running
    // the job. This is necessary for backward compatibility as other systems
    // might use the public API JobConf#setWorkingDirectory to reset the working
    // directory.
    job.getWorkingDirectory();
  }

  /**
   * Internal method for submitting jobs to the system.
   * 
   * <p>The job submission process involves:
   * <ol>
   *   <li>
   *   Checking the input and output specifications of the job.
   *   </li>
   *   <li>
   *   Computing the {@link InputSplit}s for the job.
   *   </li>
   *   <li>
   *   Setup the requisite accounting information for the 
   *   {@link DistributedCache} of the job, if necessary.
   *   </li>
   *   <li>
   *   Copying the job's jar and configuration to the map-reduce system
   *   directory on the distributed file-system. 
   *   </li>
   *   <li>
   *   Submitting the job to the <code>JobTracker</code> and optionally
   *   monitoring it's status.
   *   </li>
   * </ol></p>
   * @param job the configuration to submit
   * @param cluster the handle to the Cluster
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  JobStatus submitJobInternal(Job job, Cluster cluster) 
  throws ClassNotFoundException, InterruptedException, IOException {

    /**
     * 验证输出路径
     */
    //validate the jobs output specs 
    checkSpecs(job);

    Configuration conf = job.getConfiguration();

    /**
     * 将应用框架的路径添加到分布式缓存里
     */
    addMRFrameworkToDistributedCache(conf);

    /**
     * 作业执行过程中，和作业相关的信息都会存储在这个目录下（HDFS中）
     */
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);
    //configure the command line options correctly on the submitting dfs
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      submitHostAddress = ip.getHostAddress();
      submitHostName = ip.getHostName();
      conf.set(MRJobConfig.JOB_SUBMITHOST,submitHostName);
      conf.set(MRJobConfig.JOB_SUBMITHOSTADDR,submitHostAddress);
    }

    /**
     * 生成JobId
     */
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);

    /**
     * 提交一个Application的时候，Yarn的客户端会将该APP的一切要用的资源初始化并存储在HDFS的目录中，
     * 那么以后哪个节点要执行Task，就会从这个路径拉取资源文件
     *     1. 作业的jar包
     *     2. job.xml （作业执行过程中需要用的各种配置：eg 资源的配置）
     *     3. shell命令（最终在某个节点上启动Task的命令）
     *     ......
     */
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;
    try {
      conf.set(MRJobConfig.USER_NAME,
          UserGroupInformation.getCurrentUser().getShortUserName());
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
      LOG.debug("Configuring job " + jobId + " with " + submitJobDir 
          + " as the submit dir");
      // get delegation token for the dir
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);
      
      populateTokenCache(conf, job.getCredentials());

      // generate a secret to authenticate shuffle transfers
      if (TokenCache.getShuffleSecretKey(job.getCredentials()) == null) {
        KeyGenerator keyGen;
        try {
         
          int keyLen = CryptoUtils.isShuffleEncrypted(conf) 
              ? conf.getInt(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS, 
                  MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS)
              : SHUFFLE_KEY_LENGTH;
          keyGen = KeyGenerator.getInstance(SHUFFLE_KEYGEN_ALGORITHM);
          keyGen.init(keyLen);
        } catch (NoSuchAlgorithmException e) {
          throw new IOException("Error generating shuffle secret key", e);
        }
        SecretKey shuffleKey = keyGen.generateKey();
        TokenCache.setShuffleSecretKey(shuffleKey.getEncoded(),
            job.getCredentials());
      }

      /**
       * 上传jar包到submitJobDir目录下
       */
      copyAndConfigureFiles(job, submitJobDir);

      /**
       * 获取job.xml的地址
       */
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);

      /**
       * splits 相关的设置
       */
      // Create the splits for the job
      LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));

      /**
       *  逻辑切片
       */
      int maps = writeSplits(job, submitJobDir);

      /**
       * 设置mapTask的数量
       */
      conf.setInt(MRJobConfig.NUM_MAPS, maps);
      LOG.info("number of splits:" + maps);

      // write "queue admins of the queue to which job is being submitted"
      // to job file.
      /**
       * 作业的队列名
       * mapreduce.job.queuename,默认是default
       */
      String queue = conf.get(MRJobConfig.QUEUE_NAME,
          JobConf.DEFAULT_QUEUE_NAME);
      AccessControlList acl = submitClient.getQueueAdmins(queue);
      conf.set(toFullPropertyName(queue,
          QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

      // removing jobtoken referrals before copying the jobconf to HDFS
      // as the tasks don't need this setting, actually they may break
      // because of it if present as the referral will point to a
      // different job.
      TokenCache.cleanUpTokenReferral(conf);

      if (conf.getBoolean(
          MRJobConfig.JOB_TOKEN_TRACKING_IDS_ENABLED,
          MRJobConfig.DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED)) {
        // Add HDFS tracking ids
        ArrayList<String> trackingIds = new ArrayList<String>();
        for (Token<? extends TokenIdentifier> t :
            job.getCredentials().getAllTokens()) {
          trackingIds.add(t.decodeIdentifier().getTrackingId());
        }
        conf.setStrings(MRJobConfig.JOB_TOKEN_TRACKING_IDS,
            trackingIds.toArray(new String[trackingIds.size()]));
      }

      // Set reservation info if it exists
      ReservationId reservationId = job.getReservationId();
      if (reservationId != null) {
        conf.set(MRJobConfig.RESERVATION_ID, reservationId.toString());
      }

      /**
       * 将各种配置信息写到HDFS
       */
      // Write job file to submit dir
      writeConf(conf, submitJobFile);
      
      //
      // Now, actually submit the job (using the submit name)
      //
      printTokens(jobId, job.getCredentials());

      /**
       * 提交作业
       * submitClient == YARNRunner
       */
      status = submitClient.submitJob(
          jobId, submitJobDir.toString(), job.getCredentials());
      if (status != null) {
        return status;
      } else {
        throw new IOException("Could not launch job");
      }
    } finally {
      if (status == null) {
        LOG.info("Cleaning up the staging area " + submitJobDir);
        if (jtFs != null && submitJobDir != null)
          jtFs.delete(submitJobDir, true);

      }
    }
  }
  
  private void checkSpecs(Job job) throws ClassNotFoundException, 
      InterruptedException, IOException {
    JobConf jConf = (JobConf)job.getConfiguration();
    // Check the output specification
    if (jConf.getNumReduceTasks() == 0 ? 
        jConf.getUseNewMapper() : jConf.getUseNewReducer()) {
      org.apache.hadoop.mapreduce.OutputFormat<?, ?> output =
        ReflectionUtils.newInstance(job.getOutputFormatClass(),
          job.getConfiguration());
      output.checkOutputSpecs(job);
    } else {
      jConf.getOutputFormat().checkOutputSpecs(jtFs, jConf);
    }
  }
  
  private void writeConf(Configuration conf, Path jobFile) 
      throws IOException {
    // Write job file to JobTracker's fs        
    FSDataOutputStream out = 
      FileSystem.create(jtFs, jobFile, 
                        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }
  
  private void printTokens(JobID jobId,
      Credentials credentials) throws IOException {
    LOG.info("Submitting tokens for job: " + jobId);
    for (Token<?> token: credentials.getAllTokens()) {
      LOG.info(token);
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends InputSplit>
  int writeNewSplits(JobContext job, Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    InputFormat<?, ?> input =
      ReflectionUtils.newInstance(job.getInputFormatClass(), conf);

    /**
     * input 默认 实现 是 TextInputFormat  FileInputFormat的子类
     * InputSplit 就是一个切片对象
     *
     * 针对FileInputFormat类型的输入
     * 200个200M的文件最后启动400个MapTask
     * 200个130M的文件最后启动200个文件
     * 文件的分片标准是 128*1.1 = 140.8；
     * 只要文件的大小大于140.8，就会进行分片，分片大小默认是128m（blocksize）
     */
    List<InputSplit> splits = input.getSplits(job);

    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    /**
     * 对切片进行排序，让数据量大的任务先执行
     */
    Arrays.sort(array, new SplitComparator());
    /**
     * 讲逻辑切片的信息写到HDFS上（jobSubmitDir）
     */
    JobSplitWriter.createSplitFiles(jobSubmitDir, conf, 
        jobSubmitDir.getFileSystem(conf), array);

    /**
     * 分片的个数
     */
    return array.length;
  }
  
  private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
      Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {

    JobConf jConf = (JobConf)job.getConfiguration();
    int maps;
    if (jConf.getUseNewMapper()) {
      maps = writeNewSplits(job, jobSubmitDir);
    } else {
      maps = writeOldSplits(jConf, jobSubmitDir);
    }
    return maps;
  }
  
  //method to write splits for old api mapper.
  private int writeOldSplits(JobConf job, Path jobSubmitDir) 
  throws IOException {
    org.apache.hadoop.mapred.InputSplit[] splits =
    job.getInputFormat().getSplits(job, job.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new Comparator<org.apache.hadoop.mapred.InputSplit>() {
      public int compare(org.apache.hadoop.mapred.InputSplit a,
                         org.apache.hadoop.mapred.InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException("Problem getting input split size", ie);
        }
      }
    });
    JobSplitWriter.createSplitFiles(jobSubmitDir, job, 
        jobSubmitDir.getFileSystem(job), splits);
    return splits.length;
  }
  
  private static class SplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in compare", ie);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private void readTokensFromFiles(Configuration conf, Credentials credentials)
  throws IOException {
    // add tokens and secrets coming from a token storage file
    String binaryTokenFilename =
      conf.get("mapreduce.job.credentials.binary");
    if (binaryTokenFilename != null) {
      Credentials binary = Credentials.readTokenStorageFile(
          FileSystem.getLocal(conf).makeQualified(
              new Path(binaryTokenFilename)),
          conf);
      credentials.addAll(binary);
    }
    // add secret keys coming from a json file
    String tokensFileName = conf.get("mapreduce.job.credentials.json");
    if(tokensFileName != null) {
      LOG.info("loading user's secret keys from " + tokensFileName);
      String localFileName = new Path(tokensFileName).toUri().getPath();

      boolean json_error = false;
      try {
        // read JSON
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> nm = 
          mapper.readValue(new File(localFileName), Map.class);

        for(Map.Entry<String, String> ent: nm.entrySet()) {
          credentials.addSecretKey(new Text(ent.getKey()), ent.getValue()
              .getBytes(Charsets.UTF_8));
        }
      } catch (JsonMappingException e) {
        json_error = true;
      } catch (JsonParseException e) {
        json_error = true;
      }
      if(json_error)
        LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
    }
  }

  //get secret keys and tokens and store them into TokenCache
  private void populateTokenCache(Configuration conf, Credentials credentials) 
  throws IOException{
    readTokensFromFiles(conf, credentials);
    // add the delegation tokens from configuration
    String [] nameNodes = conf.getStrings(MRJobConfig.JOB_NAMENODES);
    LOG.debug("adding the following namenodes' delegation tokens:" + 
        Arrays.toString(nameNodes));
    if(nameNodes != null) {
      Path [] ps = new Path[nameNodes.length];
      for(int i=0; i< nameNodes.length; i++) {
        ps[i] = new Path(nameNodes[i]);
      }
      TokenCache.obtainTokensForNamenodes(credentials, ps, conf);
    }
  }

  @SuppressWarnings("deprecation")
  private static void addMRFrameworkToDistributedCache(Configuration conf)
      throws IOException {
    String framework =
        conf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, "");
    if (!framework.isEmpty()) {
      URI uri;
      try {
        uri = new URI(framework);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Unable to parse '" + framework
            + "' as a URI, check the setting for "
            + MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, e);
      }

      String linkedName = uri.getFragment();

      // resolve any symlinks in the URI path so using a "current" symlink
      // to point to a specific version shows the specific version
      // in the distributed cache configuration
      FileSystem fs = FileSystem.get(conf);
      Path frameworkPath = fs.makeQualified(
          new Path(uri.getScheme(), uri.getAuthority(), uri.getPath()));
      FileContext fc = FileContext.getFileContext(frameworkPath.toUri(), conf);
      frameworkPath = fc.resolvePath(frameworkPath);
      uri = frameworkPath.toUri();
      try {
        uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(),
            null, linkedName);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }

      DistributedCache.addCacheArchive(uri, conf);
    }
  }
}
