require 'spec_helper'

describe "database" do

    before :all do
        # nothing
    
    end

    it "should instantiate adapter" do
        db = Brewery::Database.database_with_uri('sqlite::memory:')
        db.class.should == Brewery::SqliteDatabase
    end

    it "should provide adapter name" do
        db = Brewery::Database.database_with_uri('sqlite::memory:')
        db.adapter_name.should == 'sqlite'
    end

    it "should connect to a database" do
        db = Brewery::Database.database_with_uri('sqlite::memory:')
        db.connect
    end

    describe "table creation" do
        before :each do
            @db = Brewery::Database.database_with_uri('sqlite::memory:')
            @db.connect
            @field_list = [
                        ["a_text", :text],
                        ["a_string", :string],
                        ["an_integer", :integer],
                        ["a_float", :numeric],
                        ["a_boolean", :boolean]
                     ]
        end
        
        it "should create a table" do
            @db.create_table("foo", @field_list)
        end

        it "should list existing tables" do
            @db.create_table("foo", @field_list)
            tables = @db.tables
            tables.count.should == 1
            tables.should == ["foo"]

            @db.create_table("bar", @field_list)
            tables = @db.tables
            tables.count.should == 2
            tables.sort.should == ["bar", "foo"]
        end
        
        it "should drop a table"
        it "should not allow to create a table when one already exists"
        it "should get list of table fields" do
            fields = @field_list.collect { |a| a[0] }
            @db.create_table("foo", @field_list)
            dbfields = @db.table_field_names("foo")
            dbfields.sort.should == fields.sort
        end
    end

end

