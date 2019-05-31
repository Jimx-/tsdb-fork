#include "querier/QuerierUtils.hpp"
#include "block/BlockUtils.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

#include "base/Logging.hpp"

namespace tsdb {
namespace querier {

Series::Series(const std::deque<std::shared_ptr<SeriesInterface>>& series)
    : series(series)
{}

void Series::push_back(const std::shared_ptr<SeriesInterface>& s)
{
    series.push_back(s);
}

void Series::clear() { series.clear(); }

std::shared_ptr<SeriesInterface>& Series::operator[](int i)
{
    return series[i];
}

std::shared_ptr<SeriesInterface> Series::operator[](int i) const
{
    return series[i];
}

std::shared_ptr<SeriesInterface>& Series::at(int i) { return series[i]; }

int Series::size() { return series.size(); }

bool Series::empty() { return series.empty(); }

SeriesSets::SeriesSets(
    const std::deque<std::shared_ptr<SeriesSetInterface>>& ss)
    : ss(ss)
{}

void SeriesSets::push_back(const std::shared_ptr<SeriesSetInterface>& s)
{
    ss.push_back(s);
}

void SeriesSets::clear() { ss.clear(); }

void SeriesSets::next()
{
    std::deque<std::shared_ptr<SeriesSetInterface>>::iterator it = ss.begin();
    while (it != ss.end()) {
        if (!(*it)->next())
            it = ss.erase(it);
        else
            ++it;
    }
}

void SeriesSets::next(const std::deque<int>& id)
{
    int d = 0;
    for (auto const& i : id) {
        if (i - d < ss.size()) {
            std::deque<std::shared_ptr<SeriesSetInterface>>::iterator it =
                ss.begin() + i - d;
            if (!(*it)->next()) {
                ss.erase(it);
                ++d;
            }
        }
    }
}

std::shared_ptr<SeriesSetInterface>& SeriesSets::operator[](int i)
{
    return ss[i];
}

std::shared_ptr<SeriesSetInterface> SeriesSets::operator[](int i) const
{
    return ss[i];
}

std::shared_ptr<SeriesSetInterface>& SeriesSets::at(int i) { return ss[i]; }

int SeriesSets::size() { return ss.size(); }

bool SeriesSets::empty() { return ss.empty(); }

ChunkSeriesSets::ChunkSeriesSets(
    const std::vector<std::shared_ptr<ChunkSeriesSetInterface>>& css)
    : css(css)
{}

void ChunkSeriesSets::push_back(
    const std::shared_ptr<ChunkSeriesSetInterface>& s)
{
    css.push_back(s);
}

void ChunkSeriesSets::clear() { css.clear(); }

void ChunkSeriesSets::next()
{
    std::vector<std::shared_ptr<ChunkSeriesSetInterface>>::iterator it =
        css.begin();
    while (it != css.end()) {
        if (!(*it)->next())
            it = css.erase(it);
        else
            ++it;
    }
}

void ChunkSeriesSets::next(const std::vector<int>& id)
{
    int d = 0;
    for (auto const& i : id) {
        if (i - d < css.size()) {
            std::vector<std::shared_ptr<ChunkSeriesSetInterface>>::iterator it =
                css.begin() + i - d;
            if (!(*it)->next()) {
                css.erase(it);
                ++d;
            }
        }
    }
}

std::shared_ptr<ChunkSeriesSetInterface>& ChunkSeriesSets::operator[](int i)
{
    return css[i];
}

std::shared_ptr<ChunkSeriesSetInterface> ChunkSeriesSets::
operator[](int i) const
{
    return css[i];
}

std::shared_ptr<ChunkSeriesSetInterface>& ChunkSeriesSets::at(int i)
{
    return css[i];
}

int ChunkSeriesSets::size() { return css.size(); }

bool ChunkSeriesSets::empty() { return css.empty(); }

// GroupChunkSeriesSets::GroupChunkSeriesSets(const
// std::deque<std::shared_ptr<GroupChunkSeriesSetInterface> > & css): css(css){}

// void GroupChunkSeriesSets::push_back(const
// std::shared_ptr<GroupChunkSeriesSetInterface> & s){
//     css.push_back(s);
// }

// void GroupChunkSeriesSets::clear(){
//     css.clear();
// }

// void GroupChunkSeriesSets::next(){
//     std::deque<std::shared_ptr<GroupChunkSeriesSetInterface> >::iterator it =
//     css.begin(); while(it != css.end()){
//         if(!(*it)->next())
//             it = css.erase(it);
//         else
//             ++ it;
//     }
// }

// void GroupChunkSeriesSets::next(const std::deque<int> & id){
//     int d = 0;
//     for(auto const&i: id){
//         if(i - d < css.size()){
//             std::deque<std::shared_ptr<GroupChunkSeriesSetInterface>
//             >::iterator it = css.begin() + i - d; if(!(*it)->next()){
//                 css.erase(it);
//                 ++ d;
//             }
//         }
//     }
// }

// std::shared_ptr<GroupChunkSeriesSetInterface> &
// GroupChunkSeriesSets::operator[](int i){
//     return css[i];
// }

// std::shared_ptr<GroupChunkSeriesSetInterface>
// GroupChunkSeriesSets::operator[](int i) const{
//     return css[i];
// }

// std::shared_ptr<GroupChunkSeriesSetInterface> & GroupChunkSeriesSets::at(int
// i){
//     return css[i];
// }

// int GroupChunkSeriesSets::size(){
//     return css.size();
// }

// bool GroupChunkSeriesSets::empty(){
//     return css.empty();
// }

} // namespace querier
} // namespace tsdb
