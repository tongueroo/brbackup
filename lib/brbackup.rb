#!/usr/bin/env ruby

require 'rubygems'
require 'aws/s3'
require 'yaml'
require 'pp'
require 'optparse'
require 'open4'
require 'fileutils'

module AWS::S3
  class S3Object
    def <=>(other)
      DateTime.parse(self.about['last-modified']) <=> DateTime.parse(other.about['last-modified'])
    end
  end
end

# Usage:
# Simple:
# restores bleacherreport_production and pa_stats_production from production to staging db
#   $ brbackup --restore
#
# Advanced: 
# Specifying more options
#   $ brbackup --from=prod_br --databases=[bleacherreport, pa_stats] --restore
#   $ brbackup --from=prod_ss --databases=[cmservice, a_b] --restore

# list db
# download db
# restore db
# test with pa_stats_production
module BR
  class DatabaseEngine
    def self.register_as(name)
      BR::Backups::ENGINES[name] = self
    end

    def initialize(backups)
      @backups = backups
    end

    def dump_database(name)
      raise "Implement #dump_database in #{self.class}"
    end

    def dbuser
      @backups.config[:dbuser]
    end

    def dbpass
      @backups.config[:dbpass]
    end
  end

  class Backups
    ENGINES = {}
    
    def self.run(args)
      options = {}
      
      # Build a parser for the command line arguments
      opts = OptionParser.new do |opts|
        opts.version = VERSION

        opts.banner = "Usage: brbackup [-flag] [argument]"
        opts.define_head "brbackup: clone db backups across environments"
        opts.separator '*'*80
        
        opts.on("-f", "--from ENVIRONMENT", "EY Cloud environment name want to clone from : prod_br, beta, alpha, etc") do |env|
          options[:env] = env
        end
        
        opts.on("-l", "--list-backup DATABASE", "List mysql backups for DATABASE") do |db|
          options[:db] = (db || 'all')
          options[:command] = :list
        end
        
        opts.on("-c", "--config CONFIG", "Use config file.") do |config|
          options[:config] = config
        end
        
        opts.on("-d", "--download BACKUP_INDEX", "download the backup specified by index. Run brbackup -l to get the index.") do |index|
          options[:command] = :download
          options[:index] = index
        end

        opts.on("-r", "--restore BACKUP_INDEX", "Download and apply the backup specified by index WARNING! will overwrite the current db with the backup. Run brbackup -l to get the index.") do |index|
          options[:command] = :restore
          options[:index] = index
        end

      end

      opts.parse!(args)

      options[:engine] ||= 'mysql'
      options[:config] ||= "/etc/.#{options[:engine]}.backups.yml"

      brb = new(options)

      case options[:command]
      when :list
        brb.list options[:db], true
      when :download
        brb.download(options[:index])
      when :restore
        brb.restore(options[:index])
      end
    rescue SystemExit
      exit 1
    rescue Exception => e
      $stderr.puts "An unknown exception was raised"
      $stderr.puts e.inspect
      $stderr.puts e.backtrace
      $stderr.puts
      raise
    end
    
    def initialize(options)
      engine_klass = ENGINES[options[:engine]] || raise("Invalid database engine: #{options[:engine].inspect}")
      @engine = engine_klass.new(self)

      load_config(options[:config])

      AWS::S3::Base.establish_connection!(
          :access_key_id     => config[:aws_secret_id],
          :secret_access_key => config[:aws_secret_key]
        )
      @databases = config[:databases]
      @keep = config[:keep]
      @bucket = "ey-backup-#{Digest::SHA1.hexdigest(config[:aws_secret_id])[0..11]}"
      @tmpname = "#{Time.now.strftime("%Y-%m-%dT%H:%M:%S").gsub(/:/, '-')}.sql.gz"
      @env = options[:env] || config[:env]
      FileUtils.mkdir_p '/mnt/backups'
      FileUtils.mkdir_p '/mnt/tmp'
      begin
        AWS::S3::Bucket.find(@bucket)
      rescue AWS::S3::NoSuchBucket
        AWS::S3::Bucket.create(@bucket)
      end
      
      FileUtils.mkdir_p self.backup_dir
    end
    attr_reader :config
    
    def load_config(filename)
      if File.exist?(filename)
        @config = YAML::load(File.read(filename))
      else
        $stderr.puts "You need to have a backup file at #{filename}"
        exit 1
      end
    end
    
    def new_backup
      @databases.each do |db|
        backup_database(db)
      end  
    end
    
    def backup_database(database)
      File.open("#{self.backup_dir}/#{database}.#{@tmpname}", "w") do |f|
        puts "doing database: #{database}"
        @engine.dump_database(database, f)
      end

      File.open("#{self.backup_dir}/#{database}.#{@tmpname}") do |f|
        path = "#{@env}.#{database}/#{database}.#{@tmpname}"
        AWS::S3::S3Object.store(path, f, @bucket, :access => :private)
        puts "successful backup: #{database}.#{@tmpname}"
      end
    end

    def download(index)
      idx, db = index.split(":")
      raise Error, "You didn't specify a database name: e.g. 1:rails_production" unless db

      if obj = list(db)[idx.to_i]
        filename = normalize_name(obj)
        puts "downloading: #{filename}"
        File.open(filename, 'wb') do |f|
          print "."
          obj.value {|chunk| f.write chunk }
        end
        puts
        puts "finished"
        [db, filename]
      else
        raise BackupNotFound, "No backup found for database #{db.inspect}: requested index: #{idx}"
      end
    end
    
    def restore(index)
      db, filename = download(index)
      File.open(filename) do |f|
        @engine.restore_database(db, f)
      end
    end
    
    def cleanup
      begin
        list('all',false)[0...-(@keep*@databases.size)].each do |o| 
          puts "deleting: #{o.key}"
          o.delete
        end
      rescue AWS::S3::S3Exception, AWS::S3::Error
        nil # see bucket_minder cleanup note regarding S3 consistency
      end
    end
    
    def normalize_name(obj)
      obj.key.gsub(/^.*?\//, '')
    end
    
    def find_obj(name)
      AWS::S3::S3Object.find name, @bucket
    end
    
    def list(database='all', printer = false)
      puts "Listing database backups for #{database}" if printer
      backups = []
      if database == 'all'
        @databases.each do |db|
          backups << AWS::S3::Bucket.objects(@bucket, :prefix => "#{@env}.#{db}")
        end
        backups = backups.flatten.sort
      else
        backups = AWS::S3::Bucket.objects(@bucket, :prefix => "#{@env}.#{database}").sort
      end
      if printer
        puts "#{backups.size} backup(s) found"
        backups.each_with_index do |b,i|
          puts "#{i}:#{database} #{normalize_name(b)}"
        end
      end    
      backups
    end
    
    protected
    def backup_dir
      "/mnt/tmp"
    end
    
  end
  
  class MysqlDatabase < DatabaseEngine
    register_as 'mysql'

    def dump_database(name, io)
      single_transaction = db_has_myisam?(name) ? '' : '--single-transaction'
      Open4.spawn ["mysqldump -u#{dbuser} #{password_option} #{single_transaction} #{name} | gzip -c"], :stdout => io
    end

    def db_has_myisam?(name)
      query = "SELECT 1 FROM information_schema.tables WHERE table_schema='#{name}' AND engine='MyISAM' LIMIT 1;"
      %x{mysql -u #{dbuser} #{password_option} -N -e"#{query}"}.strip == '1'
    end

    def restore_database(name, io)
      Open4.spawn ["gzip -dc | mysql -u#{dbuser} #{password_option} #{name}"], :stdin => io
    end

    def password_option
      dbpass.blank? ? "" : "-p'#{dbpass}'"
    end
  end
end