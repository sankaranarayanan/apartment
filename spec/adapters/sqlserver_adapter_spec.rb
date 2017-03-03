require 'spec_helper'
require 'apartment/adapters/sqlserver_adapter'
require 'pry'
describe Apartment::Adapters::SqlserverAdapter, database: :mssql do
  unless defined?(JRUBY_VERSION)

    subject(:adapter){ Apartment::Tenant.sqlserver_adapter config }

   
    # before{ Apartment.use_schemas = true }

    # Not sure why, but somehow using let(:tenant_names) memoizes for the whole example group, not just each test
    def tenant_names
      # ActiveRecord::Base.connection.execute("select distinct object_schema_name(object_id, 1) as name from master.sys.objects;").collect { |row| row['name'] }
      # ActiveRecord::Base.connection.select_all("select distinct object_schema_name(object_id, 1) as name from master.sys.objects").collect { |row| row['name'] }
      ActiveRecord::Base.connection.select_all("SELECT name FROM sys.databases").collect { |row| row['name'] }
    end
    
    let!(:default_tenant) { subject.switch { ActiveRecord::Base.connection.current_database } }

    context "using - the equivalent of - schemas" do
      before { Apartment.use_schemas = true }

      it_should_behave_like "a generic apartment adapter"

      describe "#default_tenant" do
        it "is set to the original db from config" do

          expect(subject.default_tenant).to eq(config[:database])
        end
      end

      describe "#init" do
        include Apartment::Spec::AdapterRequirements

        before do
          Apartment.configure do |config|
            config.excluded_models = ["Company"]
          end
        end

        it "should process model exclusions" do
          Apartment::Tenant.init
          expect(Company.table_name).to eq("#{default_tenant}.companies")
        end
      end
    end

    context "using connections" do
      before { Apartment.use_schemas = false }

      it_should_behave_like "a generic apartment adapter"
      it_should_behave_like "a generic apartment adapter able to handle custom configuration"
      it_should_behave_like "a connection based apartment adapter"
    end
  end
end
