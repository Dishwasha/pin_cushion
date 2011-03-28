class ActiveRecord::Migration
  class << self
    # CreateMTIFor establishes a multi-inheritance model structure in the database that requires no modification to ActiveRecord
    # See http://mediumexposure.com/multiple-table-inheritance-active-record/ for further details on an ActiveRecord MTI solution
    # Also see http://altrabio.github.com/CITIEsForRAILS/ for an ActiveRecord 3 MTI solution
    #
    # CreateMTIFor will accept a class that inherits from a class that inherits from ActiveRecord::Base and set up a database-level MTI
    #
    # class Media < ActiveRecord::Base
    # end
    #
    # class CD < Media
    #   set_table_name "view_cd"
    # end
    def CreateMTIFor(classname, options={})
      options[:superclass_name] = classname.superclass.to_s
      options[:class_name] = classname.to_s
      options[:supertable_name] = classname.superclass.table_name
      options[:table_name] = classname.table_name.gsub('view_','')
      options[:table_prefix] = options[:table_prefix] || "view_"
      CreateInheritedTable(options)
    end

    # CreateMTIFor should work in most cases, but if you need to override table, class, or view prefix CreatedInheritedTable can be used instead
    def CreateInheritedTable options = {}
      superclass_name = options[:superclass_name]
      class_name = options[:class_name]
      supertable_name = options[:supertable_name]
      table_name = options[:table_name]
      table_prefix = options[:table_prefix]
      foreign_key = "#{supertable_name.singularize}_id"
      exclusions = ['id',foreign_key]

      add_column(supertable_name.to_sym, "#{superclass_name.downcase.to_sym}_type", :string) unless ActiveRecord::Base.connection.columns(supertable_name).map(&:name).include?("#{superclass_name.downcase}_type")

      # A view is created based on the combined tables of the class and its superclass.  This is what ActiveRecord sees and interacts with on CREATE/UPDATE/DELETE
      # The superclass' id will be the primary id though the child class doesn't need an id and should have an index on the foreign key
      # The child class should set_table_name to whatever table_prefix + table_name is
      execute "CREATE OR REPLACE VIEW #{table_prefix + table_name} AS SELECT #{GetTableColumnsSQLPartial supertable_name}, \
        #{GetTableColumnsSQLPartial table_name, exclusions} FROM #{supertable_name}, #{table_name} WHERE #{supertable_name}.id = #{table_name}.#{foreign_key} \
        AND #{superclass_name.downcase}_type='#{class_name}' #{GetConditionsSQLPartial(options[:conditions])};"

      supertable_columns = ActiveRecord::Base.connection.columns(supertable_name).map(&:name)
      table_columns = ActiveRecord::Base.connection.columns(table_name).map(&:name)
      sequence_name = options[:sequence_name] || "#{supertable_name}_id_seq"

      # PostgreSQL's returning statement must return a sequence of columns equivalent to the view.
      execute "CREATE TYPE #{table_prefix + table_name}_type AS (#{ActiveRecord::Base.connection.columns(table_prefix + table_name).map{|column|
        column.name + ' ' + case column.type.to_s
          when 'boolean'
            'boolean'
          when 'integer'
            'integer'
          when 'string'
            'character varying(255)'
          when 'text'
            'text'
          when 'datetime'
            'timestamp without time zone'
        end
      }.join(',')});"

      # This function is used in the returning statements to return the recently inserted data in to the joined tables
      execute "CREATE OR REPLACE FUNCTION GetInserted#{superclass_name}(int8) RETURNS SETOF #{table_prefix + table_name}_type AS $$ SELECT * FROM #{table_prefix + table_name} WHERE id = $1 $$ LANGUAGE SQL;"

      # Since PostgreSQL doesn't yet support inserts on a view with table joins, we are creating a rule to intercept the INSERT and
      # manually insert the record in to the individual tables
      # Unfortunately ActiveRecord uses the RETURNING action on INSERT to get the id of the record that was just inserted
      # Since we are INSERTing in to multiple tables, PostgreSQL requires us to manually define each and every column that will be returned
      # Although it's gross, we end up selecting the data inserted column by column using the aforementioned GetInserted... function
      # This may affect inserting performance, though should be fairly negligible, so this implementation should be stress tested before use
      execute "CREATE OR REPLACE RULE #{table_prefix + table_name}_ins AS ON INSERT TO #{table_prefix + table_name} DO INSTEAD ( \
        INSERT INTO #{supertable_name} VALUES(#{supertable_columns.map{|column|
          case column
          when 'id'
            '(SELECT nextval(\'' + sequence_name + '\'))'
          when superclass_name.downcase + '_type'
            '\'' + class_name + '\''
          else
            'NEW.' + column
          end
        }.join(',')}); \
        INSERT INTO #{table_name} VALUES(#{table_columns.map{|column|
          case column
          when 'id'
            '(SELECT nextval(\'' + table_name + '_id_seq\'))'
          when foreign_key
            '(SELECT currval(\'' + sequence_name + '\'))'
          else
            'NEW.' + column
          end
        }.join(',')}) \
        RETURNING #{ActiveRecord::Base.connection.columns(table_prefix + table_name).map{|column| '(SELECT ' + column.name + ' FROM GetInserted' + superclass_name + '(currval(\'' + sequence_name + '\')))'}.join(',')});"

      # Since PostgreSQL doesn't yet support updates on a view with table joins, we are creating a rule to intercept the UPDATE and
      # manually update the records in the individual tables
      # The order of the insert is important; when the parent table was updated first, a strange issue with pessimistic locking was observed
      # on the very first update which caused ActiveRecord to throw a StableObject error when records were created that have callbacks
      # since ActiveRecord does an updated after insert for those callbacks
      execute "CREATE OR REPLACE RULE #{table_prefix + table_name}_upd AS ON UPDATE TO #{table_prefix + table_name} DO INSTEAD ( \
        UPDATE #{table_name} SET #{(table_columns - exclusions).map{|column| column + '=NEW.' + column}.join(',')} WHERE #{foreign_key}=OLD.id; \
        UPDATE #{supertable_name} SET #{(supertable_columns - ['id',superclass_name.downcase + '_type']).map{|column| column + '=NEW.' + column}.join(',')} WHERE id=OLD.id;);"

      # Since PostgreSQL doesn't yet support deletes on a view with table joins, we are creating a rule to intercept the DELETE and
      # manually delete the child table record
      execute "CREATE OR REPLACE RULE #{table_prefix + table_name}_del AS ON DELETE TO #{table_prefix + table_name} DO INSTEAD (DELETE FROM #{table_name} WHERE #{foreign_key}=OLD.id;);"

      # For some reason the CREATE RULE ... ON DELETE TO will only execute a single statement
      # We are using this function to support a delete trigger on the child table which should only be called from the aforementioned DELETE rule
      execute "CREATE FUNCTION #{table_prefix + table_name}_del_function() RETURNS trigger AS ' \
        BEGIN
          IF tg_op = ''DELETE'' THEN
            DELETE FROM #{supertable_name} WHERE id=old.#{foreign_key};
            RETURN old;
          END IF;
        END
      ' LANGUAGE plpgsql;"

      # For some reason the CREATE RULE .. ON DELETE TO will only execute a single statement
      # This trigger takes care of deleting the record from the parent table after the record on the child table is deleted via the aforementioned DELETE rule
      execute "CREATE TRIGGER #{table_prefix + table_name}_del_trigger BEFORE DELETE ON #{table_name} FOR EACH ROW EXECUTE PROCEDURE #{table_prefix + table_name}_del_function();"
    end

    def DropMTIFor(classname, options={})
      options[:superclass_name] = classname.superclass.to_s
      options[:class_name] = classname.to_s
      options[:supertable_name] = classname.superclass.table_name
      options[:table_name] = classname.table_name.gsub('view_','')
      options[:table_prefix] = options[:table_prefix] || "view_"
      DropInheritedTable(options)
    end

    def DropInheritedTable options = {}
      superclass_name = options[:superclass_name]
      class_name = options[:class_name]
      supertable_name = options[:supertable_name]
      table_name = options[:table_name]
      table_prefix = options[:table_prefix]

      execute "DROP TRIGGER #{table_prefix + table_name}_del_trigger ON #{table_name};"
      execute "DROP FUNCTION #{table_prefix + table_name}_del_function();"
      execute "DROP RULE #{table_prefix + table_name}_del ON #{table_prefix + table_name};"
      execute "DROP RULE #{table_prefix + table_name}_upd ON #{table_prefix + table_name};"
      execute "DROP RULE #{table_prefix + table_name}_ins ON #{table_prefix + table_name};"
      execute "DROP FUNCTION GetInserted#{superclass_name}(int8);"
      execute "DROP TYPE #{table_prefix + table_name}_type;"
      execute "DROP VIEW #{table_prefix + table_name};"

      remove_column(supertable_name.to_sym, "#{superclass_name.downcase.to_sym}_type")
    end

    private
    def GetTableColumnsSQLPartial table_name, exclusions = []
      ActiveRecord::Base.connection.columns(table_name).map{|column| table_name + '.' + column.name unless exclusions.include?(column.name)}.compact.join(',')
    end

    def GetConditionsSQLPartial conditions
      conditions = [conditions].compact unless conditions.kind_of?(Array)
      " AND #{conditions.size > 1 ? conditions.first : conditions.join(" AND ")}" unless conditions.empty?
    end
  end
end

module PinCushion
  def self.included(base)
    base.send :extend, ClassMethods
  end

  module ClassMethods
    def acts_as_MTI(options = {})
      if superclass == ::ActiveRecord::Base
        class_eval do
          def superclass
            self.class.superclass
          end
        end
      else
        set_table_name "view_#{self.to_s.pluralize.underscore}"
      end
      inheritance_column = "#{self.class.to_s}_type"

      self.class_eval do
        def self.base_class
          self
        end
      end
    end
  end
end

ActiveRecord::Base.send :include, PinCushion
