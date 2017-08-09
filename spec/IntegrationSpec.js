const AdapterIntegrationSpec = require('../node_modules/bass/spec/AdapterIntegrationSpec');

describe('bass-sql', AdapterIntegrationSpec('bass-sql', {
    connections: {
        default: {
            adapter: 'bass-sql',
            host: 'localhost',
            database: 'bass_sql_test',
            port: '3306',
            user: 'root',
            password: 'root',
            driver: 'mysql'
        }
    }
}));
