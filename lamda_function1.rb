# lambda_function.rb
load_paths = Dir["./vendor/bundle/ruby/3.2.0/gems/**/lib"]
$LOAD_PATH.unshift(*load_paths)
require 'logger'
require 'json'
require 'elasticsearch'
require 'searchkick'
require 'active_record'

ActiveRecord::Base.establish_connection(
  adapter: 'postgresql', # Replace with your database adapter
  database: 'the_tie_api_development', # Replace with your database name
  username: 'postgres', # Replace with your database username
  password: 'password', # Replace with your database password
  host: 'localhost', # Replace with your database host
  port: '5432' # Replace with your database port
)


def main
  event = JSON.parse("{\"id\":14959,\"tweet_id\":null,\"headline\":\"Tech giant TestComp to roll out ChatGPT competitor AI\",\"translated_headline\":null,\"date\":\"2023-04-11T11:52:30.454Z\",\"link\":\"https://cointelegraph.com/news/tech-giant-alibaba-to-roll-out-chatgpt-competitor-ai\",\"found_in_page\":null,\"filing_keyword_match\":null,\"tweet_posttime\":null,\"timestamp\":\"2023-04-11T11:52:30.454Z\",\"created_at\":\"2023-04-11T11:57:29.907Z\",\"updated_at\":\"2023-04-11T11:57:29.907Z\",\"link_hash\":\"4cb9eaa43f50af5c1c3f571b06125d5c\",\"is_hidden\":false,\"is_starred_by_the_tie\":false,\"starred_by_the_tie_user_id\":null,\"metadata\":{\"image\":\"https://images.cointelegraph.com/cdn-cgi/image/format=auto,onerror=redirect,quality=90,width=1200/https://s3.cointelegraph.com/uploads/2023-04/1b6ea7ae-95e4-4453-b758-257feffd8dcb.jpg\",\"description\":\"Chatbot will be able to communicate in English and Mandarin at the first stage, while its task scope will include turning conversations into written notes, writing emails and drafting business proposals.\"},\"is_trending_news\":false,\"grouped_headline\":null}", symbolize_names: true)
  logger = Logger.new($stdout)
  data_synchronizer = DataSynchronizer.new('http://localhost:9200', 'news_items_development')
  logger.info('Indexing data started')
  data_synchronizer.index_data(event)
  logger.info('Indexing data complete')
  { action: event[:action], status: 'success' }
rescue StandardError => e
  puts e.to_json
  { action: event[:action], status: 'failure', error: { message: e.message } }
end


# Define a class for data synchronization and search
class DataSynchronizer
  include Searchkick

  def initialize(elasticsearch_url, elasticsearch_index)
    Searchkick.client = Elasticsearch::Client.new(url: elasticsearch_url)
    @elasticsearch_index = elasticsearch_index
  end

  def index_data(data)
    searchkick_index.store(NewsItem.new(data))
  end

  private

  def searchkick_index
    Searchkick::Index.new(@elasticsearch_index)
  end
end


class NewsItem < ActiveRecord::Base
  searchkick
end

if __FILE__ == $PROGRAM_NAME
  main
end
