using NHibernate;
using NHibernate.Driver;
using NHibernate.Engine;
using NHibernate.Impl;
using NHibernate.Loader.Custom;
using NHibernate.Loader.Custom.Sql;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using NHibernate.Type;
using NHibernate.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SAR.NHibernate
{
    public class OracleManagedDriver : OracleManagedDataClientDriver
    {
        private readonly PropertyInfo _commandBindByNameSetter;
        private readonly PropertyInfo _parameterOracleDbTypeSetter;

        public OracleManagedDriver()
        {
            _commandBindByNameSetter = ReflectHelper.TypeFromAssembly(
                "Oracle.ManagedDataAccess.Client.OracleCommand", "Oracle.ManagedDataAccess", true).GetProperty("BindByName");
            _parameterOracleDbTypeSetter = ReflectHelper.TypeFromAssembly(
                "Oracle.ManagedDataAccess.Client.OracleParameter", "Oracle.ManagedDataAccess", true).GetProperty("OracleDbType");
            var enumType = ReflectHelper.TypeFromAssembly(
                "Oracle.ManagedDataAccess.Client.OracleDbType", "Oracle.ManagedDataAccess", true);
            OracleDbTypeRefCursor = Enum.Parse(enumType, "RefCursor");
        }

        public override bool SupportsMultipleQueries => true;

        public object OracleDbTypeRefCursor { get; }

        public override DbCommand CreateCommand()
        {
            return base.CreateCommand();
        }
        public override IResultSetsCommand GetResultSetsCommand(ISessionImplementor session)
        {
            return new EnhancedOracleManagedResultSetsCommand(session);
        }
        protected override void InitializeParameter(DbParameter dbParam, string name, SqlType sqlType)
        {
            // this "exotic" parameter type will actually mean output refcursor
            if (sqlType.DbType == DbType.VarNumeric)
            {
                dbParam.ParameterName = FormatNameForParameter(name);
                dbParam.Direction = ParameterDirection.Output;
                _parameterOracleDbTypeSetter.SetValue(dbParam, OracleDbTypeRefCursor, null);
            }
            else
                base.InitializeParameter(dbParam, name, sqlType);
        }

        protected override void OnBeforePrepare(DbCommand command)
        {
            base.OnBeforePrepare(command);

            if (command.CommandText.StartsWith("\nBEGIN -- multi query"))
            {
                // for better performance, in multi-queries, 
                // we switch to parameter binding by position (not by name)
                this._commandBindByNameSetter.SetValue(command, false, null);
                command.CommandText = command.CommandText.Replace(":p", ":");
            }
        }

        public class EnhancedOracleManagedResultSetsCommand : BasicResultSetsCommand
        {
            private readonly SqlStringBuilder _sqlStringBuilder = new SqlStringBuilder();
            private SqlString _sqlString = new SqlString();
            private QueryParameters _prefixQueryParameters;
            private CustomLoader _prefixLoader;

            public EnhancedOracleManagedResultSetsCommand(ISessionImplementor session)
                : base(session) { }

            public override SqlString Sql => _sqlString;

            public override void Append(ISqlCommand command)
            {
                if (_prefixLoader == null)
                {
                    var prefixQuery = (SqlQueryImpl)((ISession)Session)
                        // this SQL query fragment will prepend every SELECT query in multiquery/multicriteria 
                        .CreateSQLQuery("\nOPEN :crsr FOR\n")
                        // this "exotic" parameter type will actually mean output refcursor
                        .SetParameter("crsr", 0, new DecimalType(new SqlType(DbType.VarNumeric)));

                    _prefixQueryParameters = prefixQuery.GetQueryParameters();

                    var querySpecification = prefixQuery.GenerateQuerySpecification(_prefixQueryParameters.NamedParameters);

                    _prefixLoader = new CustomLoader(new SQLCustomQuery(querySpecification.SqlQueryReturns, querySpecification.QueryString,
                        querySpecification.QuerySpaces, Session.Factory), Session.Factory);
                }

                var prefixCommand = _prefixLoader.CreateSqlCommand(_prefixQueryParameters, Session);

                Commands.Add(prefixCommand);
                Commands.Add(command);

                _sqlStringBuilder.Add(prefixCommand.Query);
                _sqlStringBuilder.Add(command.Query).Add(";");
            }
            public override DbDataReader GetReader(int? commandTimeout)
            {
                var batcher = Session.Batcher;
                var sqlTypes = Commands.SelectMany(c => c.ParameterTypes).ToArray();
                ForEachSqlCommand((sqlLoaderCommand, offset) => sqlLoaderCommand.ResetParametersIndexesForTheCommand(offset));

                _sqlStringBuilder.Insert(0, "\nBEGIN -- multi query").Add("\nEND;-- multi query\n");
                _sqlString = _sqlStringBuilder.ToSqlString();

                var command = batcher.PrepareQueryCommand(CommandType.Text, _sqlString, sqlTypes);
                if (commandTimeout.HasValue)
                    command.CommandTimeout = commandTimeout.Value;

                BindParameters(command);

                return BatcherDataReaderWrapper.Create(batcher, command);
            }
        }
    }
}
