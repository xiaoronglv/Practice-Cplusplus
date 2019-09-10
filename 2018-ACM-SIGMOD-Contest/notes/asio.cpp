#include<iostream>
#include<boost/asio.hpp>
#include<boost/asio/io_service.hpp>


boost::asio::io_service s;


auto work = []() -> void
{
	std::cout << "I am doing well" << std::endl;
};

int main(int argc, char *argv[])
{
  
s.post(work);
s.run();
  return 0;
}


