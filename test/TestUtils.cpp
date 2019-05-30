#include <boost/tokenizer.hpp>
#include <fstream>
#include <stdlib.h>
#include "test/TestUtils.hpp"

namespace tsdb{
namespace test{

std::deque<std::deque<double>> load_sample_data(const std::string & filename){
    std::deque<std::deque<double>> r;
    std::ifstream file(filename);
    std::string line;
    boost::char_separator<char> tokenSep(",");
    while(getline(file, line)){
        r.push_back(std::deque<double>());
        boost::tokenizer<boost::char_separator<char> > tokens(line, tokenSep);
        for(auto const& token: tokens){
            r.back().push_back(atof(token.c_str()));
        }
    }
    return r;
}

}
}