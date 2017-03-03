require 'apartment/adapters/abstract_adapter'

module Apartment
  module Tenant

    def self.sqlserver_adapter(config)
      # adapter = Adapters::SqlserverAdapter
      # adapter = Adapters::SqlserverSchemaAdapter if Apartment.use_schemas
      # adapter = Adapters::SqlserverSchemaFromSqlAdapter if Apartment.use_sql && Apartment.use_schemas
      # adapter.new(config)
      Apartment.use_schemas ?
      Adapters::SqlserverSchemaAdapter.new(config) :
      Adapters::SqlserverAdapter.new(config)
    end
  end

  module Adapters
    # Default adapter when not using MsSql Schemas
    class SqlserverAdapter < AbstractAdapter
      def initialize(config)
        super

        @default_tenant = config[:database]
      end

    private

      def rescue_from
        TinyTds::Error
      end
    end

    # Separate Adapter for MsSql when using schemas
    class SqlserverSchemaAdapter < AbstractAdapter

      def initialize(config)
        super
        @default_tenant = config[:database]
        reset
      end

      #   Reset schema search path to the default schema_search_path
      #
      #   @return {String} default schema search path
      #
      def reset        
        # Apartment.connection.schema_search_path = full_search_path
        Apartment.connection.execute "use #{default_tenant}"
      end

      # def current
      #   @current || default_tenant
      # end

    protected

      def process_excluded_model(excluded_model)
        excluded_model.constantize.tap do |klass|
          # Ensure that if a schema *was* set, we override
          table_name = klass.table_name.split('.', 2).last

          klass.table_name = "#{default_tenant}.#{table_name}"
        end
      end

      # def drop_command(conn, tenant)
      #   conn.execute(%{DROP SCHEMA "#{tenant}" CASCADE})
      # end

      #   Set schema search path to new schema
      #
      def connect_to_new(tenant)
        return reset if tenant.nil?
        Apartment.connection.execute "use #{environmentify(tenant)}"

      rescue ActiveRecord::StatementInvalid => exception
        Apartment::Tenant.reset
        raise_connect_error!(tenant, exception)
      end

    private

      # def create_tenant_command(conn, tenant)
      #   conn.execute(%{CREATE SCHEMA "#{tenant}"})
      # end

      #   Generate the final search path to set including persistent_schemas
      #
      # def full_search_path        
      #   persistent_schemas.map(&:inspect).join(", ")
      # end

      # def persistent_schemas
      #   [@current, Apartment.persistent_schemas].flatten        
      # end

      # def reset_on_connection_exception?
      #   true
      # end
    end

    # Another Adapter for MsSql when using schemas and SQL
    class SqlserverSchemaFromSqlAdapter < SqlserverSchemaAdapter

    #   MSSQL_DUMP_BLACKLISTED_STATEMENTS= [
    #     /SET search_path/i,   # overridden later
    #     /SET lock_timeout/i   # new in MsSql 9.3
    #   ]

    #   def import_database_schema
    #     clone_ms_schema
    #     copy_schema_migrations
    #   end

    # private

    #   # Clone default schema into new schema named after current tenant
    #   #
    #   def clone_ms_schema
    #     ms_schema_sql = patch_search_path(ms_dump_schema)
    #     Apartment.connection.execute(ms_schema_sql)
    #   end

    #   # Copy data from schema_migrations into new schema
    #   #
    #   def copy_schema_migrations
    #     ms_migrations_data = patch_search_path(ms_dump_schema_migrations_data)
    #     Apartment.connection.execute(ms_migrations_data)
    #   end

    #   #   Dump postgres default schema
    #   #
    #   #   @return {String} raw SQL contaning only postgres schema dump
    #   #
    #   def ms_dump_schema

    #     # Skip excluded tables? :/
    #     # excluded_tables =
    #     #   collect_table_names(Apartment.excluded_models)
    #     #   .map! {|t| "-T #{t}"}
    #     #   .join(' ')

    #     # `ms_dump -s -x -O -n #{default_tenant} #{excluded_tables} #{dbname}`

    #     with_ms_env { `ms_dump -s -x -O -n #{default_tenant} #{dbname}` }
    #   end

    #   #   Dump data from schema_migrations table
    #   #
    #   #   @return {String} raw SQL contaning inserts with data from schema_migrations
    #   #
    #   def ms_dump_schema_migrations_data
    #     with_ms_env { `ms_dump -a --inserts -t schema_migrations -t ar_internal_metadata -n #{default_tenant} #{dbname}` }
    #   end

    #   # Temporary set MsSql related environment variables if there are in @config
    #   #
    #   def with_ms_env(&block)
  
    #     mshost, msport, msuser, mspassword, msmode =  ENV['MSHOST'], ENV['MSPORT'], ENV['MSUSER'], ENV['MSPASSWORD'], ENV['MSMODE']

    #     ENV['MSHOST'] = @config[:host] if @config[:host]
    #     ENV['MSPORT'] = @config[:port].to_s if @config[:port]
    #     ENV['MSUSER'] = @config[:username].to_s if @config[:username]
    #     ENV['MSPASSWORD'] = @config[:password].to_s if @config[:password]
    #     ENV['MSMODE'] = @config[:mode].to_s if @config[:mode]

    #     block.call
    #   ensure
    #     ENV['MSHOST'], ENV['MSPORT'], ENV['MSUSER'], ENV['MSPASSWORD'], ENV['MSMODE'] = mshost, msport, msuser, mspassword, msmode
    #   end

    #   #   Remove "SET search_path ..." line from SQL dump and prepend search_path set to current tenant
    #   #
    #   #   @return {String} patched raw SQL dump
    #   #
    #   def patch_search_path(sql)
    #     search_path = "SET search_path = \"#{current}\", #{default_tenant};"

    #     sql
    #       .split("\n")
    #       .select {|line| check_input_against_regexps(line, MsSQL_DUMP_BLACKLISTED_STATEMENTS).empty?}
    #       .prepend(search_path)
    #       .join("\n")
    #   end

    #   #   Checks if any of regexps matches against input
    #   #
    #   def check_input_against_regexps(input, regexps)
    #     regexps.select {|c| input.match c}
    #   end

    #   #   Collect table names from AR Models
    #   #
    #   def collect_table_names(models)
    #     models.map do |m|
    #       m.constantize.table_name
    #     end
    #   end

    #   # Convenience method for current database name
    #   #
    #   def dbname
    #     Apartment.connection_config[:database]
    #   end

      
    end
  end
end
