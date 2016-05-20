require_relative '../helper'

class TaggedCopyOutputTest < Test::Unit::TestCase
  class << self
    def startup
      spec = Gem::Specification.find { |s| s.name == 'fluentd' }
      $LOAD_PATH.unshift File.join(spec.full_gem_path, 'test', 'scripts')
      require 'fluent/plugin/out_test'
    end

    def shutdown
      $LOAD_PATH.shift
    end
  end

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    <store>
      type test
      name c0
    </store>
    <store>
      type test
      name c1
    </store>
    <store>
      type test
      name c2
    </store>
  ]

  def create_driver(conf = CONFIG, tag = 'test')
    Fluent::Test::OutputTestDriver.new(Fluent::TaggedCopyOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver

    outputs = d.instance.outputs
    assert_equal 3, outputs.size
    assert_equal Fluent::TestOutput, outputs[0].class
    assert_equal Fluent::TestOutput, outputs[1].class
    assert_equal Fluent::TestOutput, outputs[2].class
    assert_equal "c0", outputs[0].name
    assert_equal "c1", outputs[1].name
    assert_equal "c2", outputs[2].name
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    d.instance.outputs.each {|o|
      assert_equal [
          [time, {"a"=>1}],
          [time, {"a"=>2}],
        ], o.events
    }
  end

  def test_msgpack_es_emit_bug
    d = Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput)

    outputs = %w(p1 p2).map do |pname|
      p = Fluent::Plugin.new_output('test')
      p.configure('name' => pname)
      p.define_singleton_method(:emit) do |tag, es, chain|
        es.each do |time, record|
          super(tag, [[time, record]], chain)
        end
      end
      p
    end

    d.instance.instance_eval { @outputs = outputs }

    es = if defined?(MessagePack::Packer)
           time = Time.parse("2013-05-26 06:37:22 UTC").to_i
           packer = MessagePack::Packer.new
           packer.pack([time, {"a" => 1}])
           packer.pack([time, {"a" => 2}])
           Fluent::MessagePackEventStream.new(packer.to_s)
         else
           events = "#{[time, {"a" => 1}].to_msgpack}#{[time, {"a" => 2}].to_msgpack}"
           Fluent::MessagePackEventStream.new(events)
         end

    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    d.instance.outputs.each { |o|
      assert_equal [
        [time, {"a"=>1}],
        [time, {"a"=>2}],
      ], o.events
    }
  end

  def create_event_test_driver(is_deep_copy = false)
    deep_copy_config = %[
      deep_copy true
    ]

    output1 = Fluent::Plugin.new_output('test')
    output1.configure('name' => 'output1')
    output1.define_singleton_method(:emit) do |tag, es, chain|
      es.each do |time, record|
        record['foo'] = 'bar'
        super(tag, [[time, record]], chain)
      end
    end

    output2 = Fluent::Plugin.new_output('test')
    output2.configure('name' => 'output2')
    output2.define_singleton_method(:emit) do |tag, es, chain|
      es.each do |time, record|
        super(tag, [[time, record]], chain)
      end
    end

    outputs = [output1, output2]

    d = Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput)
    d = d.configure(deep_copy_config) if is_deep_copy
    d.instance.instance_eval { @outputs = outputs }
    d
  end

  def test_one_event
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(false)
    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}]],
      [[time, {"a"=>1, "foo"=>"bar"}]]
    ], d.instance.outputs.map{ |o| o.events }

    d = create_event_test_driver(true)
    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}]],
      [[time, {"a"=>1}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  def test_multi_event
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(false)
    es = Fluent::MultiEventStream.new
    es.add(time, {"a" => 1})
    es.add(time, {"b" => 2})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]],
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]]
    ], d.instance.outputs.map{ |o| o.events }

    d = create_event_test_driver(true)
    es = Fluent::MultiEventStream.new
    es.add(time, {"a" => 1})
    es.add(time, {"b" => 2})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]],
      [[time, {"a"=>1}], [time, {"b"=>2}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  ## Belows are special tests for tagged_copy

  def test_tag_emit
    config = %[
      <store>
        <filter>
          tag first
        </filter>
        type test
        name c0
      </store>
      <store>
        <filter>
          tag second
        </filter>
        type test
        name c0
      </store>
    ]
    d = create_driver(config)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    first = d.instance.outputs.first
    assert_equal [
      ['first', time, {"a"=>1}],
      ['first', time, {"a"=>2}],
    ], first.emits

    second = d.instance.outputs[1]
    assert_equal [
      ['second', time, {"a"=>1}],
      ['second', time, {"a"=>2}],
    ], second.emits
  end

  def test_add_tag_prefix_emit
    config = %[
      <store>
        <filter>
          add_tag_prefix first
        </filter>
        type test
        name c0
      </store>
      <store>
        <filter>
          add_tag_prefix second
        </filter>
        type test
        name c0
      </store>
    ]
    d = create_driver(config)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    first = d.instance.outputs.first
    assert_equal [
      ['first.test', time, {"a"=>1}],
      ['first.test', time, {"a"=>2}],
    ], first.emits

    second = d.instance.outputs[1]
    assert_equal [
      ['second.test', time, {"a"=>1}],
      ['second.test', time, {"a"=>2}],
    ], second.emits
  end

  def test_remove_tag_prefix_emit
    config = %[
      <store>
        <filter>
          remove_tag_prefix first
        </filter>
        type test
        name c0
      </store>
      <store>
        <filter>
          remove_tag_prefix second
        </filter>
        type test
        name c0
      </store>
    ]
    d = create_driver(config)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.tag = 'first.test'
    d.emit({"a"=>1}, time)
    d.tag = 'second.test'
    d.emit({"a"=>2}, time)

    first = d.instance.outputs.first
    assert_equal [
      ['test', time, {"a"=>1}],
      ['second.test', time, {"a"=>2}],
    ], first.emits

    second = d.instance.outputs[1]
    assert_equal [
      ['first.test', time, {"a"=>1}],
      ['test', time, {"a"=>2}],
    ], second.emits
  end

  def test_add_tag_suffix_emit
    config = %[
      <store>
        <filter>
          add_tag_suffix first
        </filter>
        type test
        name c0
      </store>
      <store>
        <filter>
          add_tag_suffix second
        </filter>
        type test
        name c0
      </store>
    ]
    d = create_driver(config)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    first = d.instance.outputs.first
    assert_equal [
      ['test.first', time, {"a"=>1}],
      ['test.first', time, {"a"=>2}],
    ], first.emits

    second = d.instance.outputs[1]
    assert_equal [
      ['test.second', time, {"a"=>1}],
      ['test.second', time, {"a"=>2}],
    ], second.emits
  end

  def test_remove_tag_suffix_emit
    config = %[
      <store>
        <filter>
          remove_tag_suffix first
        </filter>
        type test
        name c0
      </store>
      <store>
        <filter>
          remove_tag_suffix second
        </filter>
        type test
        name c0
      </store>
    ]
    d = create_driver(config)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.tag = 'test.first'
    d.emit({"a"=>1}, time)
    d.tag = 'test.second'
    d.emit({"a"=>2}, time)

    first = d.instance.outputs.first
    assert_equal [
      ['test', time, {"a"=>1}],
      ['test.second', time, {"a"=>2}],
    ], first.emits

    second = d.instance.outputs[1]
    assert_equal [
      ['test.first', time, {"a"=>1}],
      ['test', time, {"a"=>2}],
    ], second.emits
  end
end
