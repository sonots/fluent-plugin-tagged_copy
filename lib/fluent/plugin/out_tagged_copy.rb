require 'fluent/plugin/out_copy'

module Fluent
  class TaggedCopyOutput < CopyOutput
    Plugin.register_output('tagged_copy', self)

    def initialize
      super
      @tag_procs = []
    end 

    # Override to handle filter:tag options
    def configure(conf)
      conf.elements.select {|e|
        e.name == 'store'
      }.each {|e|
        type = e['type']
        unless type
          raise ConfigError, "Missing 'type' parameter on <store> directive"
        end 
        log.debug "adding store type=#{type.dump}"

        f = e.elements.select {|i| i.name == 'filter'}.first || {}
        tag_proc = generate_tag_proc(f['tag'], f['add_tag_prefix'], f['remove_tag_prefix'])
        @tag_procs << tag_proc

        output = Plugin.new_output(type)
        output.configure(e)
        @outputs << output
      }
    end

    # Override to use TaggedOutputChain
    def emit(tag, es, chain)
      unless es.repeatable?
        m = MultiEventStream.new
        es.each {|time,record|
          m.add(time, record)
        }   
        es = m 
      end 
      if @deep_copy
        chain = TaggedCopyOutputChain.new(@outputs, @tag_procs, tag, es, chain)
      else
        chain = TaggedOutputChain.new(@outputs, @tag_procs, tag, es, chain)
      end 
      chain.next
    end

    private

    def generate_tag_proc(tag, add_tag_prefix, remove_tag_prefix)
      tag_prefix = "#{add_tag_prefix}." if add_tag_prefix
      tag_prefix_match = "#{remove_tag_prefix}." if remove_tag_prefix
      if tag
        Proc.new {|t| tag }
      elsif tag_prefix and tag_prefix_match
        Proc.new {|t| "#{tag_prefix}#{lstrip(t, tag_prefix_match)}" }
      elsif tag_prefix_match
        Proc.new {|t| lstrip(t, tag_prefix_match) }
      elsif tag_prefix
        Proc.new {|t| "#{tag_prefix}#{t}" }
      else
        Proc.new {|t| t }
      end
    end

    def lstrip(string, substring)
      string.index(substring) == 0 ? string[substring.size..-1] : string
    end
  end 

  class TaggedOutputChain
    def initialize(array, tag_procs, tag, es, chain=NullOutputChain.instance)
      @array = array
      @tag_procs = tag_procs
      @tag = tag
      @es = es
      @offset = 0
      @chain = chain
    end   
        
    def next
      if @array.length <= @offset
        return @chain.next
      end
      @offset += 1
      emit_tag = @tag_procs[@offset-1].call(@tag) # added
      result = @array[@offset-1].emit(emit_tag, @es, self)
      result
    end
  end
      
  class TaggedCopyOutputChain < TaggedOutputChain
    def next
      if @array.length <= @offset
        return @chain.next
      end
      @offset += 1
      es = @array.length > @offset ? @es.dup : @es
      emit_tag = @tag_procs[@offset-1].call(@tag) # added
      result = @array[@offset-1].emit(emit_tag, es, self)
      result
    end
  end

end


