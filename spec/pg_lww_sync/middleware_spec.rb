require 'spec_helper'

RSpec.describe PgLwwSync::RequestRouter do
  let(:inner_app) { ->(env) { [200, {}, ['OK']] } }
  let(:router)    { described_class.new(inner_app) }

  let(:get_env)    { { 'REQUEST_METHOD' => 'GET' } }
  let(:post_env)   { { 'REQUEST_METHOD' => 'POST' } }
  let(:put_env)    { { 'REQUEST_METHOD' => 'PUT' } }
  let(:delete_env) { { 'REQUEST_METHOD' => 'DELETE' } }

  let(:primary_config) do
    { node_id: 'node_a', host: 'localhost', port: 5432,
      database: 'test_db', username: 'postgres', password: '' }
  end

  before do
    stub_config(node_id: 'node_a', remote_nodes: [remote_node(node_id: 'node_b')])
    allow(PgLwwSync::Consensus).to receive(:determine_active_primary).and_return(primary_config)
    allow(PgLwwSync).to receive(:ensure_connection_pool_registered!)
    allow(ActiveRecord::Base).to receive(:connected_to).and_yield
  end

  # -------------------------------------------------------------------------
  # Happy path
  # -------------------------------------------------------------------------
  describe '#call — happy path' do
    it 'calls the inner app and returns its response' do
      status, _, body = router.call(get_env)
      expect(status).to eq(200)
      expect(body).to eq(['OK'])
    end

    it 'determines the active primary on each request' do
      expect(PgLwwSync::Consensus).to receive(:determine_active_primary)
      router.call(get_env)
    end

    it 'registers the connection pool for the primary node via the module helper' do
      expect(PgLwwSync).to receive(:ensure_connection_pool_registered!).with(primary_config)
      router.call(get_env)
    end
  end

  # -------------------------------------------------------------------------
  # IDEMPOTENT_METHODS
  # -------------------------------------------------------------------------
  describe 'IDEMPOTENT_METHODS' do
    it 'includes GET, HEAD, and OPTIONS' do
      expect(described_class::IDEMPOTENT_METHODS).to contain_exactly('GET', 'HEAD', 'OPTIONS')
    end

    it 'does not include POST, PUT, PATCH, or DELETE' do
      %w[POST PUT PATCH DELETE].each do |m|
        expect(described_class::IDEMPOTENT_METHODS).not_to include(m)
      end
    end
  end

  # -------------------------------------------------------------------------
  # ActiveRecord::ConnectionNotEstablished — the one error Rails only raises
  # when it's certain the query was never sent, so it's safe to retry
  # regardless of HTTP method.
  # -------------------------------------------------------------------------
  describe '#call — ActiveRecord::ConnectionNotEstablished retry' do
    let(:second_config) { primary_config.merge(node_id: 'node_b') }

    before { allow(PgLwwSync::Consensus).to receive(:invalidate_fast_path!) }

    it 'retries on GET' do
      call_count = 0
      allow(ActiveRecord::Base).to receive(:connected_to) do
        call_count += 1
        raise ActiveRecord::ConnectionNotEstablished, 'refused' if call_count == 1
        [200, {}, ['OK']]
      end
      allow(PgLwwSync::Consensus).to receive(:determine_active_primary)
        .and_return(primary_config, second_config)

      expect { router.call(get_env) }.not_to raise_error
    end

    it 'retries on POST — Rails only raises this when the query was never sent' do
      call_count = 0
      allow(ActiveRecord::Base).to receive(:connected_to) do
        call_count += 1
        raise ActiveRecord::ConnectionNotEstablished, 'refused' if call_count == 1
        [200, {}, ['OK']]
      end
      allow(PgLwwSync::Consensus).to receive(:determine_active_primary)
        .and_return(primary_config, second_config)

      expect { router.call(post_env) }.not_to raise_error
    end

    it 'blacklists the failed node before retrying' do
      call_count = 0
      allow(ActiveRecord::Base).to receive(:connected_to) do
        call_count += 1
        raise ActiveRecord::ConnectionNotEstablished, 'gone' if call_count == 1
        [200, {}, ['OK']]
      end

      expect(PgLwwSync::Consensus).to receive(:determine_active_primary)
        .with(blacklist: []).and_return(primary_config)
      expect(PgLwwSync::Consensus).to receive(:determine_active_primary)
        .with(blacklist: ['node_a']).and_return(second_config)

      router.call(get_env)
    end

    it 're-raises after exhausting all nodes' do
      allow(ActiveRecord::Base).to receive(:connected_to)
        .and_raise(ActiveRecord::ConnectionNotEstablished, 'all down')

      expect { router.call(get_env) }.to raise_error(ActiveRecord::ConnectionNotEstablished)
    end
  end

  # -------------------------------------------------------------------------
  # PG::ConnectionBad — ambiguous (may have fired mid-query on some Rails
  # versions/paths), so only retried for idempotent methods, same as
  # StatementInvalid — NOT the same treatment as ConnectionNotEstablished.
  # -------------------------------------------------------------------------
  describe '#call — PG::ConnectionBad retry' do
    before { allow(PgLwwSync::Consensus).to receive(:invalidate_fast_path!) }

    context 'GET request' do
      it 'retries and succeeds' do
        call_count = 0
        allow(ActiveRecord::Base).to receive(:connected_to) do
          call_count += 1
          raise PG::ConnectionBad, 'refused' if call_count == 1
          [200, {}, ['OK']]
        end
        expect { router.call(get_env) }.not_to raise_error
      end
    end

    context 'POST request' do
      it 'does not retry — raises immediately to prevent a possible duplicate write' do
        allow(ActiveRecord::Base).to receive(:connected_to)
          .and_raise(PG::ConnectionBad, 'gone')

        expect { router.call(post_env) }.to raise_error(PG::ConnectionBad)
      end

      it 'logs a warning about the non-retry' do
        allow(ActiveRecord::Base).to receive(:connected_to)
          .and_raise(PG::ConnectionBad, 'gone')

        expect(Rails.logger).to receive(:error).with(/Not retrying POST/)
        router.call(post_env) rescue nil
      end
    end

    %w[PUT DELETE].each do |method|
      context "#{method} request" do
        it 'does not retry' do
          allow(ActiveRecord::Base).to receive(:connected_to)
            .and_raise(PG::ConnectionBad, 'gone')

          expect { router.call({ 'REQUEST_METHOD' => method }) }
            .to raise_error(PG::ConnectionBad)
        end
      end
    end
  end

  # -------------------------------------------------------------------------
  # ActiveRecord::ConnectionFailed — only defined on some Rails versions.
  # Same idempotent-only treatment as PG::ConnectionBad/StatementInvalid.
  # -------------------------------------------------------------------------
  describe '#call — ActiveRecord::ConnectionFailed retry', if: defined?(ActiveRecord::ConnectionFailed) do
    before { allow(PgLwwSync::Consensus).to receive(:invalidate_fast_path!) }

    it 'is included in AMBIGUOUS_CONNECTION_ERRORS' do
      expect(described_class::AMBIGUOUS_CONNECTION_ERRORS).to include(ActiveRecord::ConnectionFailed)
    end

    it 'retries on GET' do
      call_count = 0
      allow(ActiveRecord::Base).to receive(:connected_to) do
        call_count += 1
        raise ActiveRecord::ConnectionFailed, 'mid-query' if call_count == 1
        [200, {}, ['OK']]
      end
      expect { router.call(get_env) }.not_to raise_error
    end

    it 'does not retry on POST' do
      allow(ActiveRecord::Base).to receive(:connected_to)
        .and_raise(ActiveRecord::ConnectionFailed, 'mid-query')

      expect { router.call(post_env) }.to raise_error(ActiveRecord::ConnectionFailed)
    end
  end



  # -------------------------------------------------------------------------
  # StatementInvalid — only retryable for idempotent methods
  # -------------------------------------------------------------------------
  describe '#call — StatementInvalid retry' do
    before { allow(PgLwwSync::Consensus).to receive(:invalidate_fast_path!) }

    context 'GET request' do
      it 'retries and succeeds' do
        call_count = 0
        allow(ActiveRecord::Base).to receive(:connected_to) do
          call_count += 1
          raise ActiveRecord::StatementInvalid, 'dropped' if call_count == 1
          [200, {}, ['OK']]
        end
        expect { router.call(get_env) }.not_to raise_error
      end
    end

    context 'POST request' do
      it 'does not retry — raises immediately to prevent duplicate writes' do
        allow(ActiveRecord::Base).to receive(:connected_to)
          .and_raise(ActiveRecord::StatementInvalid, 'mid-tx failure')

        expect { router.call(post_env) }.to raise_error(ActiveRecord::StatementInvalid)
      end

      it 'logs a warning about the non-retry' do
        allow(ActiveRecord::Base).to receive(:connected_to)
          .and_raise(ActiveRecord::StatementInvalid, 'mid-tx failure')

        expect(Rails.logger).to receive(:error).with(/Not retrying POST/)
        router.call(post_env) rescue nil
      end
    end

    %w[PUT DELETE].each do |method|
      context "#{method} request" do
        it 'does not retry' do
          allow(ActiveRecord::Base).to receive(:connected_to)
            .and_raise(ActiveRecord::StatementInvalid, 'mid-tx failure')

          expect { router.call({ 'REQUEST_METHOD' => method }) }
            .to raise_error(ActiveRecord::StatementInvalid)
        end
      end
    end
  end

  # -------------------------------------------------------------------------
  # ensure_pool_registered! delegates to module
  # -------------------------------------------------------------------------
  describe '#ensure_pool_registered! (private)' do
    it 'delegates to PgLwwSync.ensure_connection_pool_registered!' do
      # Use the real implementation (not the stub from before)
      allow(PgLwwSync).to receive(:ensure_connection_pool_registered!).and_call_original
      allow(ActiveRecord::Base.connection_handler).to receive(:retrieve_connection_pool)
        .and_return(double('pool'))

      expect(PgLwwSync).to receive(:ensure_connection_pool_registered!).with(primary_config)
      router.send(:ensure_pool_registered!, primary_config)
    end
  end

  # -------------------------------------------------------------------------
  # POOL_REGISTRATION_MUTEX lives on PgLwwSync module
  # -------------------------------------------------------------------------
  describe 'PgLwwSync::POOL_REGISTRATION_MUTEX' do
    it 'exists on the PgLwwSync module (not RequestRouter)' do
      expect(PgLwwSync::POOL_REGISTRATION_MUTEX).to be_a(Mutex)
    end

    it 'is not defined on RequestRouter' do
      expect { described_class::POOL_REGISTRATION_MUTEX }.to raise_error(NameError)
    end
  end
end
