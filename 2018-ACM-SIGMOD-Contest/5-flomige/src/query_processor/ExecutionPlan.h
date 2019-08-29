// SIGMOD Programming Contest 2018 Submission
// Copyright (C) 2018  Florian Wolf, Michael Brendle, Georgios Psaropoulos
//
// This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with this program; if not, see
// <http://www.gnu.org/licenses/>.

#ifndef EXECUTION_PLAN_H
#define EXECUTION_PLAN_H

#include "Pipeline.h"

#include "../database/Database.h"

#include <algorithm> // std::remove
#include <bitset>
#include <fstream> // filestream for createRuntimeLatexTree

#include <iostream> // TODO remove

// #define PRINT_PLAN_CREATION_INFO
#ifdef PRINT_PLAN_CREATION_INFO
    #include <iostream>
#endif

namespace query_processor {

    // filter meta data for a table
    struct FilterMetaData {
        enum Comparison : char { Less='<', Greater='>', Equal='=' };
        // filter column
        std::string _columnPermanentName;
        // constant
        uint64_t _constant;
        // comparison type
        Comparison _comparison;

        FilterMetaData(std::string columnPermanentName, uint64_t constant, FilterMetaData::Comparison comparison)
        : _columnPermanentName(columnPermanentName), _constant(constant), _comparison(comparison) {
        }
    };

    // regular input struct for a table
    struct TableInput {
        std::string _permanentTableName;
        std::string _aliasTableName;
        std::vector<FilterMetaData> _filterPredicates;
        std::vector<ColumnInput> _usedColumns;
        TableInput(
            std::string permanentTableName,
            std::string aliasTableName = std::string()
        ) : _permanentTableName(permanentTableName),
            _aliasTableName( (aliasTableName.empty()?permanentTableName:aliasTableName)),
            _filterPredicates(),
            _usedColumns()
        {
            // ensure the members have the right format to avoid issues while query processing
            basis::Utilities::validName(_permanentTableName);
            basis::Utilities::validName(_aliasTableName);
        }
        // add a filter predicate
        void addFilterPredicate(FilterMetaData& filterPredicate) {
            basis::Utilities::validName(filterPredicate._columnPermanentName);
            _filterPredicates.push_back(filterPredicate);
        }
        // add a permanent column name that is used during the complete query
        void addUsedColumn(std::string& usedColumnPermanentName, bool isProjectionColumn = false) {
            basis::Utilities::validName(usedColumnPermanentName);
            // check if this column already exists
            for (ColumnInput& column : _usedColumns) {
                // if column is already used, increase the operator count
                if (column._columnPermanentName == usedColumnPermanentName) {
                    column._operatorCount++;
                    column._isProjectionColumn |= isProjectionColumn;
                    if (isProjectionColumn) {
                        column._projectionCount++;
                    }
                    return;
                }
            }
            // column not used yet, add it to the used column
            _usedColumns.emplace_back(usedColumnPermanentName, isProjectionColumn);
        }
    };

    // table input containing a value for the estimated cardinality
    struct TableInputWithStats : public TableInput {
        uint64_t _estimatedCardinalityBaseTable;
        uint64_t _estimatedCardinalityAfterFilters;
        TableInputWithStats(
            std::string permanentTableName,
            uint64_t estimatedCardinalityBaseTable,
            uint64_t estimatedCardinalityAfterFilters,
            std::string aliasTableName = std::string()
        ) : TableInput(permanentTableName,aliasTableName), _estimatedCardinalityBaseTable(estimatedCardinalityBaseTable),
            _estimatedCardinalityAfterFilters(estimatedCardinalityAfterFilters) {
        }
        TableInputWithStats(TableInput tableInput, uint64_t estimatedCardinalityBaseTable, uint64_t estimatedCardinalityAfterFilters)
        : TableInput(tableInput), _estimatedCardinalityBaseTable(estimatedCardinalityBaseTable), _estimatedCardinalityAfterFilters(estimatedCardinalityAfterFilters){
        }
    };

    // base for table meta data
    template<uint32_t TABLE_PARTITION_SIZE>
    struct TableMetaBase{
        std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> _permanentTablePointer;
        std::bitset<64> _optimizerTableId;
        TableMetaBase(std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> permanentTablePointer, std::bitset<64> optimizerTableId)
        : _permanentTablePointer(permanentTablePointer), _optimizerTableId(optimizerTableId){
        }
    };

    // internal table representation in the query optimizer, for estimations with profile propagation
    template<uint32_t TABLE_PARTITION_SIZE>
    struct TableMetaProfiles : public TableInput, public TableMetaBase<TABLE_PARTITION_SIZE> {
        TableMetaProfiles(const TableInput& tableInput, std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> permanentTablePointer, std::bitset<64> optimizerTableId)
        : TableInput(tableInput), TableMetaBase<TABLE_PARTITION_SIZE>(permanentTablePointer, optimizerTableId){
        }
    };

    // internal table representation in the query optimizer, for simplified estimations without profile propagation
    template<uint32_t TABLE_PARTITION_SIZE>
    struct TableMetaSimplified : public TableInputWithStats, public TableMetaBase<TABLE_PARTITION_SIZE> {
        TableMetaSimplified(
            const TableInputWithStats& tableInput,
            std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> permanentTablePointer,
            std::bitset<64> optimizerTableId)
        : TableInputWithStats(tableInput), TableMetaBase<TABLE_PARTITION_SIZE>(permanentTablePointer, optimizerTableId){
        }
        TableMetaSimplified(
            const TableInput& tableInput,
            uint64_t estimatedCardinalityBaseTable,
            uint64_t estimatedCardinalityAfterFilters,
            std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> permanentTablePointer,
            std::bitset<64> optimizerTableId)
        : TableInputWithStats(tableInput, estimatedCardinalityBaseTable, estimatedCardinalityAfterFilters),
          TableMetaBase<TABLE_PARTITION_SIZE>(permanentTablePointer, optimizerTableId){
        }
    };


    // regular input struct for a binary joins
    struct JoinAttributeInput {
        std::string _leftTableAliasName;
        std::string _leftColumnPermanentName;
        std::string _rightTableAliasName;
        std::string _rightColumnPermanentName;
        bool _isPrimaryKeyForeignKeyJoin;
        JoinAttributeInput(
            std::string leftTableAliasName,
            std::string leftColumnPermanentName,
            std::string rightTableAliasName,
            std::string rightColumnPermanentName,
            bool isPrimaryKeyForeignKeyJoin = false)
        : _leftTableAliasName(leftTableAliasName),
          _leftColumnPermanentName(leftColumnPermanentName),
          _rightTableAliasName(rightTableAliasName),
          _rightColumnPermanentName(rightColumnPermanentName),
          _isPrimaryKeyForeignKeyJoin(isPrimaryKeyForeignKeyJoin)
        {
            // ensure the members have the right format to avoid issues while query processing
            basis::Utilities::validName(_leftTableAliasName);
            basis::Utilities::validName(_leftColumnPermanentName);
            basis::Utilities::validName(_rightTableAliasName);
            basis::Utilities::validName(_rightColumnPermanentName);
        }

        bool operator==(const JoinAttributeInput& other) {
            // check for regular side
            if (this->_leftTableAliasName == other._leftTableAliasName && this->_leftColumnPermanentName == other._leftColumnPermanentName
                && this->_rightTableAliasName == other._rightTableAliasName && this->_rightColumnPermanentName == other._rightColumnPermanentName)
            {
                return true;
            }
            // check for other side
            if (this->_leftTableAliasName == other._rightTableAliasName && this->_leftColumnPermanentName == other._rightColumnPermanentName
                && this->_rightTableAliasName == other._leftTableAliasName && this->_rightColumnPermanentName == other._leftColumnPermanentName)
            {
                return true;
            }
            return false;
        }
    };

    // join input containing a join selectivity estimate, avoids that the optimizer estimates the selectivity based on database statistics
    struct JoinAttributeInputWithStats : public JoinAttributeInput {
        double _estimatedSelectivity;
        JoinAttributeInputWithStats(
            std::string leftTableAliasName,
            std::string leftColumnPermanentName,
            std::string rightTableAliasName,
            std::string rightColumnPermanentName,
            double estimatedSelectivity,
            bool isPrimaryKeyForeignKeyJoin = false)
        : JoinAttributeInput(leftTableAliasName, leftColumnPermanentName, rightTableAliasName, rightColumnPermanentName, isPrimaryKeyForeignKeyJoin),
          _estimatedSelectivity(estimatedSelectivity){
        }
        JoinAttributeInputWithStats(
            const JoinAttributeInput& joinAttributeInput,
            double estimatedSelectivity)
        : JoinAttributeInput(joinAttributeInput), _estimatedSelectivity(estimatedSelectivity){
        }
    };

    // base for join attribute meta data structs
    struct JoinAttributeMetaBase {
        std::bitset<64> _joinBits; // contains two bits that represents the two tables that are joined (_joinBits = _leftTableId | _rightTableId)
        std::bitset<64> _leftTableId; // indicate which bit is the left hand side table
        std::bitset<64> _rightTableId; // indicate which bit is the right hand side table
        JoinAttributeMetaBase(
            std::bitset<64> joinBits,
            std::bitset<64> leftTableId,
            std::bitset<64> rightTableId)
        : _joinBits(joinBits), _leftTableId(leftTableId), _rightTableId(rightTableId){
        }
    };

    // internal join representation in the query optimizer, for estimations with profile propagation
    struct JoinAttributeMetaProfiles : public JoinAttributeInput, public JoinAttributeMetaBase {
        JoinAttributeMetaProfiles(
            const JoinAttributeInput& joinAttributeInput,
            std::bitset<64> joinBits,
            std::bitset<64> leftTableId,
            std::bitset<64> rightTableId)
        : JoinAttributeInput(joinAttributeInput), JoinAttributeMetaBase(joinBits, leftTableId, rightTableId){
        }
        void switchLeftAndRightSide(){
            std::swap(_leftTableAliasName, _rightTableAliasName);
            std::swap(_leftColumnPermanentName, _rightColumnPermanentName);
            std::swap(_leftTableId, _rightTableId);
        }
    };

    // internal join representation in the query optimizer, for simplified estimations without profile propagation
    struct JoinAttributeMetaSimplified : public JoinAttributeInputWithStats, public JoinAttributeMetaBase {
        JoinAttributeMetaSimplified(
            const JoinAttributeInputWithStats& joinAttributeInput,
            std::bitset<64> joinBits,
            std::bitset<64> leftTableId,
            std::bitset<64> rightTableId)
        : JoinAttributeInputWithStats(joinAttributeInput), JoinAttributeMetaBase(joinBits, leftTableId, rightTableId){
        }
        JoinAttributeMetaSimplified(
            const JoinAttributeInput& joinAttributeInput,
            double estimatedSelectivity,
            std::bitset<64> joinBits,
            std::bitset<64> leftTableId,
            std::bitset<64> rightTableId)
        : JoinAttributeInputWithStats(joinAttributeInput, estimatedSelectivity), JoinAttributeMetaBase(joinBits, leftTableId, rightTableId){
        }
        void switchLeftAndRightSide(){
            std::swap(_leftTableAliasName,_rightTableAliasName);
            std::swap(_leftColumnPermanentName,_rightColumnPermanentName);
            std::swap(_leftTableId,_rightTableId);
        }
    };


    // forward declaration for plan class nodes that contain a breaker
    template<uint32_t TABLE_PARTITION_SIZE>
    class BreakerNodeBase;


    // each node represents a pipeline, and has a pointer to the pipeline breaker
    template<uint32_t TABLE_PARTITION_SIZE>
    class ExecutionGraphNode {
        private:
            std::string _pipelineStartTableAliasName;
            std::vector<std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>>> _children;
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> _breakerNode;
            uint32_t _degree = 0;
            bool _isExecuted = false;

            bool isNextBreakerCandidate(int currentMaxDegree) const {
                return ((!_isExecuted) && (static_cast<int>(_degree) > currentMaxDegree));
            }

        public:

            ExecutionGraphNode()
            : _pipelineStartTableAliasName("<unknown>"){
            }

            ExecutionGraphNode(const std::string& pipelineStartTableAliasName)
            : _pipelineStartTableAliasName(pipelineStartTableAliasName){
            }

            void addChild(std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> child){
                _children.push_back(child);
            }

            void addUpDegree(ExecutionGraphNode<TABLE_PARTITION_SIZE>* buildSide){
                _degree += 1;
                _degree += buildSide->_degree;
            }

            void setBreakerNode(std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> breakerNode){
                _breakerNode = breakerNode;
            }

            void setExecuted(){
                _isExecuted = true;
            }

            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findNextPipeline() const {
                // this is the case when we invoke this function on the top node that was already executed
                // otherwise we would go to this node
                if(_isExecuted){
                    return nullptr;
                }
                // iterate over the children
                std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> deepDiveChild = nullptr;
                int currentMaxDegree = -1;
                for(std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> child : _children){
                    if(child->isNextBreakerCandidate(currentMaxDegree)){
                        currentMaxDegree = child->_degree;
                        deepDiveChild = child;
                    }
                }
                // deep dive into the child with the highest degree that was not yet executed
                if(deepDiveChild != nullptr){
                    return deepDiveChild->findNextPipeline();
                }
                // if there is no such child
                else{
                    // ensure there is a pointer to a breaker node set
                    if(_breakerNode == nullptr){
                        throw std::runtime_error("Detected inconsistency in pipeline execution graph");
                    }
                    // return the breaker node
                    // std::cout << _pipelineStartTableAliasName << std::endl;
                    return _breakerNode;
                }
            }

            std::string toString() const {
                std::string ret = "Node:" + _pipelineStartTableAliasName + "(";
                if(_isExecuted){
                    ret = "Nodx:" + _pipelineStartTableAliasName + "(";
                }
                bool first = true;
                for(std::shared_ptr<ExecutionGraphNode>& child : _children){
                    if(!first){
                        ret += ", ";
                    }
                    first = false;
                    ret += child->toString();
                }
                ret += ")";
                return ret;
            };

    };


    // base for all nodes in a query execution plan, there is no decicated execution plan class since each top
    // node in an execution plan represents the exeution plan, and we avoid another level of inderection
    template<uint32_t TABLE_PARTITION_SIZE>
    class PlanNodeBase {
        protected:
            uint64_t _outputCardinality;
            uint64_t _cost;
            std::bitset<64> _planClassId;
            // each node creates at least one starter or one operator, this member stores runtime statistics from the created starter/operator
            // stored are the statistics of the outputed intermediate result, for breakers those are statistics about the stored intermediate result

        public:
            // constructor, each plan node has an output cardinality, and there are cost until this node
            PlanNodeBase(uint64_t outputCardinality, uint64_t cost, std::bitset<64> planClassId)
            : _outputCardinality(outputCardinality), _cost(cost), _planClassId(planClassId){
            }

            // virtual destructor
            virtual ~PlanNodeBase(){
            }

            // returns the cost of the plan represented by this node
            // TODO this should be renamed to estimated sth
            uint64_t getCost() const {
                return _cost;
            }

            // returns the output cardinality of this node
            // TODO this should be renamed to estimated sth
            uint64_t getOutputCardinality() const {
                return _outputCardinality;
            }

            // returns the id of the plan class that is represented by this sub plan
            std::bitset<64> getPlanClassId() const {
                return _planClassId;
            }
            // returns the corresponding fill color according to the estimation error between the both cardinalities
            void getEstimationErrorStringAndFillColor(uint64_t estimatedCardinality, uint64_t trueCardinality,
                    std::string& estimationErrorString, std::string& estimationErrorFillColorString) const {
                // determine the smaller and bigger cardinality
                uint64_t smallerCardinality = std::min(estimatedCardinality, trueCardinality);
                uint64_t biggerCardinality = std::max(estimatedCardinality, trueCardinality);
                // case 1: both cardinalities are equal. hence, the factor is 1 (use the color white)
                if (smallerCardinality == biggerCardinality) {
                    estimationErrorString = "\\texttt{+}" + basis::Utilities::formatWithCommas(1.0);
                    estimationErrorFillColorString = "yellow!0";
                }
                // case 2: the smaller cardinality is zero (and the bigger cardinality is not zero). hence, the factor is infinity (use the red color for factor > 100)
                else if (smallerCardinality == 0) {
                    estimationErrorString = "$\\infty$";
                    estimationErrorFillColorString = "red";
                }
                // case 3: the estimation error is between 1 and infinity
                else {
                    // calculate the estimation error factor by dividing the bigger cardinality by the smaller cardinality
                    double estimationErrorFactor = (biggerCardinality * 1.0) / (smallerCardinality * 1.0);
                    // map the estimation error factor [1,100] to the fill color saturation [0, 100]
                    double fillColorSaturation = estimationErrorFactor * (100.0 / 99.0) - (100.0 / 99.0);
                    if (trueCardinality < estimatedCardinality) {
                        estimationErrorString = "\\texttt{-}" + basis::Utilities::formatWithCommas(estimationErrorFactor);
                    } else {
                        estimationErrorString = "\\texttt{+}" + basis::Utilities::formatWithCommas(estimationErrorFactor);
                    }
                    // case 3a: the saturation is below or equal 100, then use the calculated saturation
                    if (fillColorSaturation <= 100.0) {
                        estimationErrorFillColorString = "yellow!" + std::to_string(fillColorSaturation);
                    }
                    // case 3b: the saturation is bigger than 100, then use the red color for factor > 100
                    else {
                        estimationErrorFillColorString = "red";
                    }
                }
            }


            // creates a string of a latex document with tikz code that represents the query tree
            std::string createLatexString(std::string caption, bool isJoinStatisticPrinted = true, bool isEdgeLabelWide = true, bool isEdgeLabelSloped = false) const {
                // create the target string
                std::string target;
                target += "\\documentclass[border=2mm]{standalone}\n";
                // font encoding
                target += "\\usepackage[T1]{fontenc}\n";
                // use the forest environment to automatically draw the tree
                target += "\\usepackage{forest}\n";
                // need the amsmath package for the cases inside a tikz node
                target += "\\usepackage{amsmath}\n";
                // need the fdsymbol package for the reopt symbol
                target += "\\usepackage{fdsymbol}\n";
                // for circle accent over true cardinality and true cost
                target += "\\usepackage{accents}\n";
                //
                target += "\\definecolor{yellow}{RGB}{255, 255, 0}\n";
                target += "\\definecolor{red}{RGB}{255, 50, 50}\n";
                target += "\\begin{document}\n";
                // tikz style for the edge label nodes aka stats nodes
                if (isEdgeLabelSloped) {
                    target += "\\tikzstyle{stats-root-node}=[pos=0.0, align=center, inner xsep=1pt, inner ysep=1pt, text=black, font=\\normalsize, minimum width=1cm]\n";
                    target += "\\tikzstyle{stats-node}=[midway, sloped, align=center, inner xsep=1pt, inner ysep=1pt, text=black, font=\\normalsize, minimum width=1cm]\n";
                } else {
                    target += "\\tikzstyle{stats-node}=[pos=0.65, align=center, inner xsep=1pt, inner ysep=1pt, text=black, font=\\normalsize, minimum width=1cm]\n";
                }
                // tikz style for the join nodes
                if (isJoinStatisticPrinted) {
                    // with join statistics we print the all information without any shape
                    target += "\\tikzstyle{join-node}=[draw=none, font=\\normalsize, minimum height=1cm]\n";
                } else {
                    // without join statistics we just print the join sign with a bigger font size in a circle node
                    target += "\\tikzstyle{join-node}=[circle, font=\\LARGE, minimum height=1cm]\n";
                }
                // tikz style for the base table nodes
                target += "\\tikzstyle{base-table-node}=[font=\\normalsize, minimum width=2cm, minimum height=1cm]\n";
                target += "\\tikzstyle{filter-node}=[draw=none, font=\\normalsize, minimum height=1cm]\n";
                target += "\\begin{forest}\n";
                // global tree settings: text in the center of a node, use the base for the anchor, 2pt vertical and 5pt horizontal space between the text
                // and the node border, 4.5cm sibling distance, 3.5cm level distance, a gray border and a 0.5pt line width
                target += "for tree={align=center, anchor=base, inner xsep=5pt, inner ysep=2pt, text=black, s sep=4.5cm, l sep=3.5cm, draw=gray, line width=0.5pt},\n";
                // draw the invisible root node in the top
                if (isEdgeLabelSloped) {
                    target += "[, minimum width=0cm, minimum height=0cm, draw=none, l sep=1.5cm\n";
                } else {
                    target += "[, minimum width=0cm, minimum height=0cm, draw=none\n";
                }
                // draw the tree
                getTikzString(0, true, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
                // draw the end of the invisible node
                target += "]\n";
                // draw the legend
                // legend border
                // if the join statistics are printed, then we have to add the selectivity into the legend as well. since the items in the legend are then at
                // a different position we have to add a 'yOffset' in this case for the items that are at a higher position then.
                uint32_t yOffset = 0;
                if (isJoinStatisticPrinted) {
                    yOffset = 20;
                }
                target += "\\node[anchor=north west, draw=gray, line width=1pt, minimum width=215pt, minimum height="
                     + std::to_string(126 + yOffset) + "pt] at ([yshift=-10pt]current bounding box.south west) {};\n";
                // estimated and true cardinality
                target += "\\node[anchor=base east, inner sep=0mm] at ([xshift=30pt, yshift=" + std::to_string(110 + yOffset) + "pt]current bounding box.south west)";
                target += "{$\\hat{f}$, $\\accentset{\\circ}{f}$};\n";
                target += "\\node[anchor=base west, inner sep=0mm] at ([xshift=38pt, yshift=" + std::to_string(110 + yOffset) + "pt]current bounding box.south west)";
                target += "{estimated cardinality, true cardinality};\n";
                // absolute estimation error aka delta f
                target += "\\node[anchor=base east, inner sep=0mm] at ([xshift=30pt, yshift=" + std::to_string(90 + yOffset) + "pt]current bounding box.south west)";
                target += "{$\\Delta f$};\n";
                target += "\\node[anchor=base west, inner sep=0mm] at ([xshift=38pt, yshift=" + std::to_string(90 + yOffset) + "pt]current bounding box.south west)";
                target += "{$= \\: \\accentset{\\circ}{f} - \\hat{f}$};\n";
                // relative estimation error explanation
                target += "\\node[anchor=base east, inner sep=0mm] at ([xshift=30pt, yshift="
                     + std::to_string(65 + yOffset) + "pt]current bounding box.south west) {q-err.};\n";
                target += "\\node[anchor=base west, align=left, inner sep=0mm] at ([xshift=38pt, yshift="
                     + std::to_string(65 + yOffset) + "pt]current bounding box.south west)";
                target += "{$= \\: \\begin{cases} \\hspace{6pt}\\accentset{\\circ}{f} / \\hat{f} & \\mbox{if } \\accentset{\\circ}{f} \\geq \\hat{f} \\\\- \\hat{f} / \\accentset{\\circ}{f} & \\mbox{else} \\end{cases}$};\n";
                // relative estimation error color bar
                target += "\\node[anchor=base west, fill=red, minimum width = 20pt, minimum height=10pt]";
                target += "at ([xshift=38pt, yshift=" + std::to_string(40 + yOffset) + "]current bounding box.south west) {};\n";
                target += "\\node[anchor=base west, shade, left color=yellow!100, right color=yellow!0, minimum width = 60pt, minimum height=10pt] (bar1)";
                target += "at ([xshift=58pt, yshift=" + std::to_string(40 + yOffset) + "pt]current bounding box.south west) {};\n";
                target += "\\node[anchor=base west, shade, left color=yellow!0, right color=yellow!100, minimum width = 60pt, minimum height=10pt] (bar2)";
                target += "at ([xshift=118pt, yshift=" + std::to_string(40 + yOffset) + "pt]current bounding box.south west) {};\n";
                target += "\\node[anchor=base west, fill=red, minimum width = 20pt, minimum height=10pt]";
                target += "at ([xshift=178pt, yshift=" + std::to_string(40 + yOffset) + "pt]current bounding box.south west) {};\n";
                target += "\\node[anchor=base west, minimum width = 160pt, minimum height=10pt, draw=gray, line width=0.5pt] (border)";
                target += "at ([xshift=38pt, yshift=" + std::to_string(40 + yOffset) + "pt]current bounding box.south west) {};\n";
                target += "\\draw[draw=gray, line width=0.5pt] ([yshift=-5pt]bar1.north west) -- (bar1.south west);\n";
                target += "\\draw[draw=gray, line width=0.5pt] ([yshift=-5pt]bar2.north west) -- (bar2.south west);\n";
                target += "\\draw[draw=gray, line width=0.5pt] ([yshift=-5pt]bar2.north east) -- (bar2.south east);\n";
                target += "\\node[anchor=base, inner sep=0mm] at ([yshift=-11pt]bar1.south west) {-100};\n";
                target += "\\node[anchor=base, inner sep=0mm] at ([yshift=-11pt]bar2.south west) {1};\n";
                target += "\\node[anchor=base, inner sep=0mm] at ([yshift=-11pt]bar2.south east) {100};\n";
                if (isJoinStatisticPrinted) {
                    target += "\\node[anchor=base east, inner sep=0mm] at ([xshift=30pt, yshift=26pt]current bounding box.south west)";
                    target +=  "{$\\hat{s}$, $\\accentset{\\circ}{s}$};\n";
                    target += "\\node[anchor=base west, inner sep=0mm] at ([xshift=38pt, yshift=26pt]current bounding box.south west)";
                    target += "{estimated selectivity, true selectivity};\n";
                }
                target += "\\node[anchor=base east, inner sep=0mm] at ([xshift=30pt, yshift=10pt]current bounding box.south west)";
                target +=  "{$\\hat{c}$, $\\accentset{\\circ}{c}$};\n";
                target += "\\node[anchor=base west, inner sep=0mm] at ([xshift=38pt, yshift=10pt]current bounding box.south west)";
                target += "{estimated cost, true cost};\n";
                // draw the caption
                target += "\\node[anchor=north, font=\\LARGE] at ([yshift=22pt]current bounding box.north) {";
                target += basis::Utilities::replaceUnderscoreLatex(caption) + "};\n";
                // finish the tikz drawing and the document
                target += "\\end{forest}\n";
                target += "\\end{document}\n";
                // return the string
                return target;
            }

            // pure virtual function to get a string representation of the plan represented by this node
            virtual std::string toString() const = 0;

            // pure virtual function to derive a pipeline dependency graph
            // TODO const!?
            virtual std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> createExecutionGraph() = 0;

            // pure virtual function that returns the breaker of the deepest unexecuted pipeline below this node, not const because of return type
            virtual std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findDeepestUnexecutedPipeline() = 0;

            // pure virtual function to add a corresponding operator to the given pipeline
            virtual void addOperatorToPipeline(std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>>& pipeline, uint32_t& executedOperatorsCount) = 0;

            // pure virtual function to find/list all sub plans of a given plan
            // recursive function invoked on the top node of an optimal plan to list all the dependency plan classes for optimality range calculation
            virtual void findAllSubPlans(std::vector<std::pair<std::bitset<64>,std::string>>& allSubPlans) const = 0;

            // pure virtual function to recursively calculate the max depth of a query execution plan
            virtual void getMaxDepth(uint32_t& depth) const = 0;

            // pure virtual function to save memory, deallocates executed operators (usually hash tables), their runtime statistics
            // have been saved in '_runtimeStatistics' (in case it was set) by 'OperatorBase::setRuntimeStatistics'
            virtual void deallocateOperators(bool first) = 0;

            // pure virtual function to get this node's true output cardinality after execution
            virtual uint64_t getTrueCardinality() const = 0;

            // pure virtual function to recursively calculate this node's true cost after execution
            virtual uint64_t getTrueCost() const = 0;

            // pure virtual function to mark a plan switch that was detected by the executor, needed for latex/tikz visualization
            virtual void setPlanSwitched(bool isFirstInvocation) = 0;

            // pure virtual function to get a 'tikz' string of this tree, if executed it contains true cardinality, true selectivity and true cost
            virtual void getTikzString(
                uint32_t levelCount,
                bool isBuildSide,
                bool isJoinStatisticPrinted,
                bool isEdgeLabelWide,
                bool isEdgeLabelSloped,
                std::string& target
            ) const = 0;
    };

    // forward declaration
    template<uint32_t TABLE_PARTITION_SIZE>
    class IntermediateResultNode;

    // base class for all nodes that contain a pipeline breaker
    template<uint32_t TABLE_PARTITION_SIZE>
    class BreakerNodeBase : public PlanNodeBase<TABLE_PARTITION_SIZE> {
        public:
            // constructor just passing parameters to the parent class
            BreakerNodeBase(uint64_t outputCardinality, uint64_t cost, std::bitset<64> planClassId)
            : PlanNodeBase<TABLE_PARTITION_SIZE>(outputCardinality, cost, planClassId){
            }

            // virtual destructor
            ~BreakerNodeBase(){
            }

            // pure virtual function enforces each breaker to provide runtime feedback e.g. the physical breaker containing the intermediate result
            // and the up-to-date statistics
            virtual void getRuntimeFeedback(std::bitset<64>& executedPlanClassId, uint64_t& estimatedCardinality) const = 0;

            // pure virtual function to get the intermediate result from a breaker to link it in the plan table
            virtual void getIntermediateResult(std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>>& intermediateResultBreaker) = 0;
    };


    // query execution plan node for base tables
    template<uint32_t TABLE_PARTITION_SIZE>
    class BaseTableNode : public PlanNodeBase<TABLE_PARTITION_SIZE> {
        private:
            TableMetaSimplified<TABLE_PARTITION_SIZE> _tableMeta;
            uint64_t _outputCardinalityBaseTable; // _outputCardinality = _estimatedCardinalityAfterFilters
            std::unique_ptr<RuntimeStatistics> _runtimeStatisticsBaseTable = nullptr;
            std::unique_ptr<RuntimeStatistics> _runtimeStatisticsAfterFilters = nullptr;

        public:
            // constructor taking the corresponding base table meta data, output cardinality is taken from the table meta data,
            // cost for base tables are zero by definition
            BaseTableNode(std::bitset<64> planClassId, const TableMetaSimplified<TABLE_PARTITION_SIZE>& tableMeta)
            : PlanNodeBase<TABLE_PARTITION_SIZE>(tableMeta._estimatedCardinalityAfterFilters, tableMeta._estimatedCardinalityAfterFilters, planClassId),
              _tableMeta(tableMeta), _outputCardinalityBaseTable(tableMeta._estimatedCardinalityBaseTable) {
            }

            // provide the meta data of the base table that is associated with this plan node
            const TableMetaSimplified<TABLE_PARTITION_SIZE>& getBaseTableMeta() const {
                return _tableMeta;
            }

            // implementation of interface function, returns the alias name of the base table
            std::string toString() const {
                return _tableMeta._aliasTableName;
            }

            // implementation of interface function to derive a pipeline dependency graph
            std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> createExecutionGraph(){
                return std::make_shared<ExecutionGraphNode<TABLE_PARTITION_SIZE>>(_tableMeta._aliasTableName);
            }

            // implementation of interface function, 'BaseTableNode' is the base case for this recursive function,
            // stops the recursion and just returns a nullptr to indicate that there is no breaker node below this node
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findDeepestUnexecutedPipeline(){
                return nullptr;
            }

            // implementation of interface function, 'BaseTableNode' is the base case for this recursive function,
            // creates a pipeline on the represented base table so that the invoking nodes can add their operators
            void addOperatorToPipeline(std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>>& pipeline, uint32_t&){
                _runtimeStatisticsBaseTable = std::make_unique<RuntimeStatistics>();
                pipeline = std::make_shared<ExecutablePipeline<TABLE_PARTITION_SIZE>>(
                    _tableMeta._permanentTablePointer, _tableMeta._aliasTableName,
                    _tableMeta._usedColumns, _runtimeStatisticsBaseTable.get()
                );
                #ifdef PRINT_PLAN_CREATION_INFO
                    std::cout << "created new pipeline on " << _tableMeta._aliasTableName << std::endl;
                #endif
                // add filter predicates
                if (!_tableMeta._filterPredicates.empty()) {
                    _runtimeStatisticsAfterFilters = std::make_unique<RuntimeStatistics>();
                    for (FilterMetaData& filterPredicate : _tableMeta._filterPredicates) {
                        switch (filterPredicate._comparison) {
                            case FilterMetaData::Comparison::Less : {
                                pipeline->addLessFilterOperator(_tableMeta._aliasTableName + "." + filterPredicate._columnPermanentName,
                                    filterPredicate._constant, _runtimeStatisticsAfterFilters.get());
                                break;
                            };
                            case FilterMetaData::Comparison::Equal : {
                                pipeline->addEqualFilterOperator(_tableMeta._aliasTableName + "." + filterPredicate._columnPermanentName,
                                    filterPredicate._constant, _runtimeStatisticsAfterFilters.get());
                                break;
                            };
                            case FilterMetaData::Comparison::Greater : {
                                pipeline->addGreaterFilterOperator(_tableMeta._aliasTableName + "." + filterPredicate._columnPermanentName,
                                    filterPredicate._constant, _runtimeStatisticsAfterFilters.get());
                                break;
                            };
                            default : throw std::runtime_error("This filter predicate is not supported.");
                        }
                    }
                    // add projection operator
                    pipeline->addProjectionOperator();
                }
            }

            // implementation of interface function, just return this node since a base table does not have any subplans
            void findAllSubPlans(std::vector<std::pair<std::bitset<64>,std::string>>& allSubPlans) const {
                // just add this plan to the container
                allSubPlans.emplace_back(PlanNodeBase<TABLE_PARTITION_SIZE>::_planClassId, toString());
            }

            // implementation of interface function to recursively calculate the max depth of a query execution plan
            void getMaxDepth(uint32_t& depth) const {
                // just increment the depth and return
                depth++;
            }

            // implementation of interface function to save memory by deallocating executed operators
            void deallocateOperators(bool){
                // this is the recursion base, we don't need to deallocate any operators here so we just return
                return;
            }

            // implementation of interface function to get this node's true output cardinality after execution
            uint64_t getTrueCardinality() const {
                if(_runtimeStatisticsBaseTable == nullptr){
                    throw std::runtime_error("Tried to access base table node's runtime statistics although node was not executed 1");
                }
                if (_runtimeStatisticsAfterFilters == nullptr) {
                    return _runtimeStatisticsBaseTable->_trueCardinality;
                }
                return _runtimeStatisticsAfterFilters->_trueCardinality;
            }

            // implementation of interface function to recursively calculate this node's true cost after execution
            uint64_t getTrueCost() const {
                return getTrueCardinality();
            }

            // implementation of interface function to mark a plan switch that was detected by the executor, needed for latex/tikz visualization
            void setPlanSwitched(bool isFirstInvocation){
                if(isFirstInvocation){
                    std::runtime_error("Detected Inconsistency. This can be only invoked on breakers.");
                }
                if(_runtimeStatisticsBaseTable == nullptr){
                    throw std::runtime_error("Tried to access base table node's runtime statistics although node was not executed 2");
                }
                // TODO check runtime statistics
                _runtimeStatisticsAfterFilters->_causedPlanSwitch = true;
                // _runtimeStatisticsBaseTable->_causedPlanSwitch = true;
            }

            // implementation of interface function to get a 'tikz' string of this tree, if executed it contains true cardinality, true selectivity and true cost
            void getTikzString(
                uint32_t levelCount,
                bool isBuildSide,
                bool,
                bool isEdgeLabelWide,
                bool isEdgeLabelSloped,
                std::string& target
            ) const {
                // to achieve a readable latex file, spaces in each line depending on the current level of the tree are needed
                std::string sep;
                for (uint32_t i = 0; i <= levelCount; ++i) {
                    sep += "    "; // 4 spaces
                }
                // determine the strings for true cardinality, absolute cardinality difference, estimation error difference,
                // for the fill color of the relative estimation error difference and for the true cost
                std::string trueCardinalityBaseTableString;
                std::string trueCardinalityDifferenceBaseTableString;
                std::string estimationErrorBaseTableString;
                std::string estimationErrorFillColorBaseTableString;
                std::string estimatedCostBaseTableString;
                std::string trueCostBaseTableString;
                if(_runtimeStatisticsBaseTable == nullptr){
                    trueCardinalityBaseTableString = "-";
                    trueCardinalityDifferenceBaseTableString = "-";
                    estimationErrorBaseTableString = "-";
                    estimationErrorFillColorBaseTableString = "yellow!0";
                    trueCostBaseTableString = "-";
                }
                else{
                    // true cardinality
                    trueCardinalityBaseTableString = basis::Utilities::formatWithCommas(_runtimeStatisticsBaseTable->_trueCardinality);
                    // absolute cardinality difference
                    if (_runtimeStatisticsBaseTable->_trueCardinality >= this->_outputCardinalityBaseTable) {
                        trueCardinalityDifferenceBaseTableString = "\\texttt{+}" + basis::Utilities::formatWithCommas(
                            _runtimeStatisticsBaseTable->_trueCardinality - this->_outputCardinalityBaseTable);
                    } else {
                        trueCardinalityDifferenceBaseTableString = "\\texttt{-}" + basis::Utilities::formatWithCommas(
                            this->_outputCardinalityBaseTable - _runtimeStatisticsBaseTable->_trueCardinality);
                    }
                    // estimation error difference and the fill color of the relative estimation error difference
                    PlanNodeBase<TABLE_PARTITION_SIZE>::getEstimationErrorStringAndFillColor(this->_outputCardinalityBaseTable, _runtimeStatisticsBaseTable->_trueCardinality,
                        estimationErrorBaseTableString, estimationErrorFillColorBaseTableString);
                    // estimated and true cost
                    if (_tableMeta._filterPredicates.empty()) {
                        estimatedCostBaseTableString = basis::Utilities::formatWithCommas(this->_cost);
                        trueCostBaseTableString = basis::Utilities::formatWithCommas(_runtimeStatisticsBaseTable->_trueCardinality);
                    } else {
                        estimatedCostBaseTableString = basis::Utilities::formatWithCommas(0);
                        trueCostBaseTableString = basis::Utilities::formatWithCommas(0);
                    }
                }
                // check if there are filter predicates
                if (!_tableMeta._filterPredicates.empty()) {
                    // determine the strings for true cardinality, absolute cardinality difference, estimation error difference,
                    // for the fill color of the relative estimation error difference and for the true cost
                    std::string trueCardinalityAfterFiltersString;
                    std::string trueCardinalityDifferenceAfterFiltersString;
                    std::string estimationErrorAfterFiltersString;
                    std::string estimationErrorFillColorAfterFiltersString;
                    std::string trueCostAfterFiltersString;
                    std::string edgeString;
                    if(_runtimeStatisticsAfterFilters == nullptr){
                        trueCardinalityAfterFiltersString = "-";
                        trueCardinalityDifferenceAfterFiltersString = "-";
                        estimationErrorAfterFiltersString = "-";
                        estimationErrorFillColorAfterFiltersString = "yellow!0";
                        trueCostAfterFiltersString = "-";
                        edgeString = "edge={line width=0.5pt}";
                    }
                    else {
                        // true cardinality
                        trueCardinalityAfterFiltersString = basis::Utilities::formatWithCommas(_runtimeStatisticsAfterFilters->_trueCardinality);
                        // absolute cardinality difference
                        if (_runtimeStatisticsAfterFilters->_trueCardinality >= this->_outputCardinality) {
                            trueCardinalityDifferenceAfterFiltersString = "\\texttt{+}" + basis::Utilities::formatWithCommas(
                                _runtimeStatisticsAfterFilters->_trueCardinality - this->_outputCardinality);
                        } else {
                            trueCardinalityDifferenceAfterFiltersString = "\\texttt{-}" + basis::Utilities::formatWithCommas(
                                this->_outputCardinality - _runtimeStatisticsAfterFilters->_trueCardinality);
                        }
                        // estimation error difference and the fill color of the relative estimation error difference
                        PlanNodeBase<TABLE_PARTITION_SIZE>::getEstimationErrorStringAndFillColor(this->_outputCardinality, _runtimeStatisticsAfterFilters->_trueCardinality,
                            estimationErrorAfterFiltersString, estimationErrorFillColorAfterFiltersString);
                        // true cost
                        trueCostAfterFiltersString = basis::Utilities::formatWithCommas(_runtimeStatisticsAfterFilters->_trueCardinality);
                        // edge string
                        if (_runtimeStatisticsAfterFilters->_causedPlanSwitch) {
                            // if the plan was switched we print a dotted line instead of a solid line
                            edgeString = "edge={dotted, line width=1pt}";
                        } else {
                            edgeString = "edge={line width=0.5pt}";
                        }
                    }
                    // print the name into the node
                    target += sep + "[";
                    for (uint32_t i = 0; i < _tableMeta._filterPredicates.size(); ++i) {
                        if (i > 0) {
                            target += "\\\\";
                        }
                        target += _tableMeta._aliasTableName + "." + _tableMeta._filterPredicates[i]._columnPermanentName;
                        switch (_tableMeta._filterPredicates[i]._comparison) {
                            case FilterMetaData::Comparison::Less : target += " < "; break;
                            case FilterMetaData::Comparison::Equal : target += " < "; break;
                            case FilterMetaData::Comparison::Greater : target += " > "; break;
                            default : throw std::runtime_error("This filter predicate is not supported.");
                        }
                        target += std::to_string(_tableMeta._filterPredicates[i]._constant);
                    }
                    // print the statistics into the edge above the node
                    target += ", " + edgeString + ", filter-node, ";
                    // edge label shape
                    if (isEdgeLabelSloped && levelCount == 0) {
                        target += "edge label={[xshift=5pt]node[stats-root-node, anchor=south";
                    } else {
                        if (isBuildSide) {
                            target += "edge label={[xshift=10pt]node[right, stats-node, ";
                            if (isEdgeLabelSloped) {
                                target += "anchor=south, ";
                            } else {
                                target += "anchor=base west, ";
                            }
                        } else {
                            target += "edge label={[xshift=-10pt]node[left, stats-node, ";
                            if (isEdgeLabelSloped) {
                                target += "anchor=south, ";
                            } else {
                                target += "anchor=base east, ";
                            }
                        }
                    }
                    // fill color of the edge label depending on the estimation error
                    target += "fill=" + estimationErrorFillColorAfterFiltersString + "]{";
                    if (isEdgeLabelWide) {
                        // in the landscape mode we print two statistics into one line
                        target += "$\\hat{f}$: " + basis::Utilities::formatWithCommas(this->_outputCardinality) + " / $\\accentset{\\circ}{f}$: " + trueCardinalityAfterFiltersString + "\\\\ "
                            + "$\\Delta f$: " + trueCardinalityDifferenceAfterFiltersString + " / $q$-error: " + estimationErrorAfterFiltersString + "\\\\ "
                            + "$\\hat{c}$: " + basis::Utilities::formatWithCommas(this->_cost) + " / $\\accentset{\\circ}{c}$: " + trueCostAfterFiltersString + "}}\n";
                    } else {
                        // in the horizontal mode we print every statistics into one line
                        target += "$\\hat{f}$: " + basis::Utilities::formatWithCommas(this->_outputCardinality) + " \\\\ $\\accentset{\\circ}{f}$: " + trueCardinalityAfterFiltersString + "\\\\ "
                            + "$\\Delta f$: " + trueCardinalityDifferenceAfterFiltersString + " \\\\ $q$-error: " + estimationErrorAfterFiltersString + "\\\\ "
                            + "$\\hat{c}$: " + basis::Utilities::formatWithCommas(this->_cost) + " \\\\ $\\accentset{\\circ}{c}$: " + trueCostAfterFiltersString + "}}\n";
                    }
                    // add additional sep for the base table node
                    target += "    ";
                }
                // print the name into the node
                target += sep + "[" + basis::Utilities::replaceUnderscoreLatex(_tableMeta._aliasTableName) + ", ";
                // print the statistics into the edge above the node
                target += "edge={line width=0.5pt}, base-table-node, ";
                // edge label shape
                if (isEdgeLabelSloped && levelCount == 0) {
                    target += "edge label={[xshift=5pt]node[stats-root-node, anchor=south";
                } else {
                    if (isBuildSide) {
                        target += "edge label={[xshift=10pt]node[right, stats-node, ";
                        if (isEdgeLabelSloped) {
                            target += "anchor=south, ";
                        } else {
                            target += "anchor=base west, ";
                        }
                    } else {
                        target += "edge label={[xshift=-10pt]node[left, stats-node, ";
                        if (isEdgeLabelSloped) {
                            target += "anchor=south, ";
                        } else {
                            target += "anchor=base east, ";
                        }
                    }
                }
                // fill color of the edge label depending on the estimation error
                target += "fill=" + estimationErrorFillColorBaseTableString + "]{";
                if (isEdgeLabelWide) {
                    // in the landscape mode we print two statistics into one line
                    target += "$\\hat{f}$: " + basis::Utilities::formatWithCommas(this->_outputCardinalityBaseTable) + " / $\\accentset{\\circ}{f}$: " + trueCardinalityBaseTableString + "\\\\ "
                        + "$\\Delta f$: " + trueCardinalityDifferenceBaseTableString + " / $q$-error: " + estimationErrorBaseTableString + "\\\\ "
                        + "$\\hat{c}$: " + estimatedCostBaseTableString + " / $\\accentset{\\circ}{c}$: " + trueCostBaseTableString + "}}]\n";
                } else {
                    // in the horizontal mode we print every statistics into one line
                    target += "$\\hat{f}$: " + basis::Utilities::formatWithCommas(this->_outputCardinalityBaseTable) + " \\\\ $\\accentset{\\circ}{f}$: " + trueCardinalityBaseTableString + "\\\\ "
                        + "$\\Delta f$: " + trueCardinalityDifferenceBaseTableString + " \\\\ $q$-error: " + estimationErrorBaseTableString + "\\\\ "
                        + "$\\hat{c}$: " + estimatedCostBaseTableString + " \\\\ $\\accentset{\\circ}{c}$: " + trueCostBaseTableString + "}}]\n";
                }
                // add end of filter node
                if (!_tableMeta._filterPredicates.empty()) {
                    target += sep + "]\n";
                }
            }
    };


    // execution plan node for adaptive query processing, containing a pipeline breaker with an intermediate result
    template<uint32_t TABLE_PARTITION_SIZE>
    class IntermediateResultNode : public PlanNodeBase<TABLE_PARTITION_SIZE> {
        private:
            // members for the breaker that contains the intermediate result, and a pointer to the original plan that resulted in the intermediate result
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> _breaker;
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> _originalSubPlan;

        public:
            // constructor taking the plan class id and the runtime carinality for the parent class,
            // and the intermediate result breaker and the original plan to be stored in this class
            IntermediateResultNode(
                std::bitset<64> planClassId,
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> breaker,
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> originalSubPlan
            )
            // this is a crucial point: which cost will the executed plan get?
            // option 1 (reoptimize and maybe throw intermediate results away): same cost as base table i.e. 'originalSubPlan->getTrueCardinality()',
            //   problem with option 1 is that it ruins the estimated vs true cost comparison since the estimated cost do not
            //   consider the sub plan anymore but the true cost will
            //   TODO for this strategy (throw intermediate results), the base tables and intermediate results should have cost of '0' !?
            // : PlanNodeBase(originalSubPlan->getTrueCardinality(), runtimeCardinality, planClassId), _breaker(breaker), _originalSubPlan(originalSubPlan) {
            //
            // option 2 (reoptimize only the rest of the plan): in this case all re-enumerated plans will have the same subplan and we
            //   can give it any cost e.g. the true cost 'getTrueCost()'
            : PlanNodeBase<TABLE_PARTITION_SIZE>(originalSubPlan->getTrueCardinality(), originalSubPlan->getTrueCost(), planClassId), _breaker(breaker), _originalSubPlan(originalSubPlan) {
            }

            // implementation of the interface function, just forwards the call to the '_originalSubPlan'
            std::string toString() const {
                // return the string of the original plan
                return _originalSubPlan->toString();
            }

            // implementation of interface function to derive a pipeline dependency graph
            std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> createExecutionGraph(){
                return _originalSubPlan->createExecutionGraph();
            }
            // implementation of interface function, behaves like a base table, return nullptr since this is no breaker node and
            // there is also no breaker node below this node
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findDeepestUnexecutedPipeline() {
                // like in a base table node, return null since there is no unexecuted breaker below this operator
                return nullptr;
            }
            // implementation of interface function, behaves like a base table except that we create a pipeline that starts on the '_breaker'
            // 'executedOperatorsCount' is not affected by this function
            void addOperatorToPipeline(std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>>& pipeline, uint32_t&) {
                // just create a new executable pipeline on the '_breaker'
                pipeline = std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>>( new ExecutablePipeline<TABLE_PARTITION_SIZE>(_breaker) );
                #ifdef PRINT_PLAN_CREATION_INFO
                    std::cout << "created new pipeline on breaker of: " << _originalSubPlan->toString() << std::endl;
                #endif
            }

            // returns the '_breaker', this is invoked for example in hash joins to check if the breaker is potentially already a
            // hash join build breaker on the right column
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> getBreaker() const {
                return _breaker;
            }

            // basically invoked to remove the ownership of the breaker from this node, invoked when an operator was created that took the ownership of this node
            // ideally we should use unique pointers for this use case, but since they cannot be casted we need this work around
            void removeIntermediateResultOwnership(){
                _breaker = nullptr;
            }

            // implementation of interface function, just throws an exception since optimality ranges and adaptive execution are not supposed to be used together
            void findAllSubPlans(std::vector<std::pair<std::bitset<64>,std::string>>&) const {
                throw std::runtime_error("Finding all plans i.e. optimality ranges conceptually not supported in intermediate result nodes i.e. adaptive execution");
            }

            // implementation of interface function to recursively calculate the max depth of a query execution plan
            void getMaxDepth(uint32_t& depth) const {
                // this is treated as a base table, just increment the depth and return
                // alternatively we can also recusivley invoke this function on '_originalSubPlan'
                depth++;
            }

            // implementation of interface function to save memory by deallocating executed operators
            void deallocateOperators(bool){
                // we do not have to check the bool since this is never the first node
                _breaker = nullptr;
            }

            // implementation of interface function to get this node's true output cardinality after execution
            uint64_t getTrueCardinality() const {
                return _originalSubPlan->getTrueCardinality();
            }

            // implementation of interface function to recursively calculate this node's true cost after execution
            uint64_t getTrueCost() const {
                return _originalSubPlan->getTrueCost();
            }

            // implementation of interface function to mark a plan switch that was detected by the executor, needed for latex/tikz visualization
            void setPlanSwitched(bool isFirstInvocation){
                _originalSubPlan->setPlanSwitched(isFirstInvocation);
            }

            // implementation of interface function to get a 'tikz' string of this tree, if executed it contains true cardinality, true selectivity and true cost
            void getTikzString(
                uint32_t levelCount,
                bool isBuildSide,
                bool isJoinStatisticPrinted,
                bool isEdgeLabelWide,
                bool isEdgeLabelSloped,
                std::string& target
            ) const {
                _originalSubPlan->getTikzString(levelCount, isBuildSide, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
            }
    };


    // query execution plan node representing a hash join i.e. an inner equi join
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinNode : public BreakerNodeBase<TABLE_PARTITION_SIZE>, public std::enable_shared_from_this<HashJoinNode<TABLE_PARTITION_SIZE>> {
        private:
            double _estimatedJoinSelectivity;
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> _leftOptimalPlan;
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> _rightOptimalPlan;
            bool _isLeftSideBuildSide;
            bool _wasPhysicalEnumeration; // this has an impact on the cost function
            std::vector<JoinAttributeMetaSimplified> _joinAttributes;
            std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> _buildBreaker = nullptr;
            std::unique_ptr<RuntimeStatistics> _runtimeStatistics = nullptr;

        public:
            // constructor taking the estimated output cardinality, the cost to this point, as well as pointers the
            // left and right hand side optimal plans and the join information, it is essential that the 'leftOptimalPlan'
            // corresponds to the left side in the 'joinAttributeMeta' and vice versa
            HashJoinNode(
                uint64_t outputCardinality,
                uint64_t cost,
                std::bitset<64> planClassId,
                double estimatedJoinSelectivity,
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> leftOptimalPlan,
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> rightOptimalPlan,
                bool isLeftSideBuildSide,
                bool wasPhysicalEnumeration, // TODO could this be a template parameter!?
                const std::vector<JoinAttributeMetaSimplified>& joinAttributes)
            : BreakerNodeBase<TABLE_PARTITION_SIZE>(outputCardinality, cost, planClassId),
                _estimatedJoinSelectivity(estimatedJoinSelectivity),
                _leftOptimalPlan(leftOptimalPlan),
                _rightOptimalPlan(rightOptimalPlan),
                // the decision could be made at pipeline creation time since we do not need this decision for every created hash join node, we just
                // do it here because of 'toString()' needs it, the engine can also update this decision when it finds an appropriate hash table
                // we do not search for appropriate hash tables here since searching for each created hash join node would be to much overhead
                _isLeftSideBuildSide(isLeftSideBuildSide),
                _wasPhysicalEnumeration(wasPhysicalEnumeration),
                _joinAttributes(joinAttributes){
            }

            // implementation of the interface function, recursively invoked 'toString()' of
            // left and right hand side plans and returns a combined string
            std::string toString() const {
                // get strings of left an right plan
                std::string leftString = _leftOptimalPlan->toString();
                std::string rightString = _rightOptimalPlan->toString();
                // create versions of the strings without leading '(' to compare relation names
                std::string leftStringCmp(leftString);
                leftStringCmp.erase(std::remove(leftStringCmp.begin(), leftStringCmp.end(), '('), leftStringCmp.end());
                std::string rightStringCmp(rightString);
                rightStringCmp.erase(std::remove(rightStringCmp.begin(), rightStringCmp.end(), '('), rightStringCmp.end());
                // create string to return
                if(leftStringCmp < rightStringCmp){
                    return std::string("(") + leftString + (_isLeftSideBuildSide?"_join ":" join_") + rightString + ")";
                }
                return std::string("(") + rightString + (_isLeftSideBuildSide?" join_":"_join ") + leftString + ")";
            }


            // implementation of interface function to derive a pipeline dependency graph
            std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> createExecutionGraph(){
                // determine pointer to build and probe side
                PlanNodeBase<TABLE_PARTITION_SIZE>* buildSidePlan;
                PlanNodeBase<TABLE_PARTITION_SIZE>* probeSidePlan;
                if(_isLeftSideBuildSide){
                    buildSidePlan = _leftOptimalPlan.get();
                    probeSidePlan = _rightOptimalPlan.get();
                }
                else{
                    buildSidePlan = _rightOptimalPlan.get();
                    probeSidePlan = _leftOptimalPlan.get();
                }

                // get probe and build side execution graphs
                std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> probeSideExecution = probeSidePlan->createExecutionGraph();
                std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> buildSideExecution = buildSidePlan->createExecutionGraph();

                // check if the build side was already executed to set a corresponding flag
                bool isBuildSideExecuted = true;
                // if there is no build breaker yet
                if(_buildBreaker == nullptr){
                    isBuildSideExecuted = false;
                    // check if we already have an intermediate result node with a default breaker on the build side
                    IntermediateResultNode<TABLE_PARTITION_SIZE>* buildSideIntermediateResultNode = dynamic_cast<IntermediateResultNode<TABLE_PARTITION_SIZE>*>(buildSidePlan);
                    if(buildSideIntermediateResultNode != nullptr){
                        // TODO this check is essentially redundant to the second check, each hash join build breaker is always a default breaker
                        // check if we have already a corresponding hash join build breaker on the build side
                        // HashJoinEarlyBreakerBase* tempBuildBreaker = dynamic_cast<HashJoinEarlyBreakerBase*>(buildSideIntermediateResultNode->getBreaker().get());
                        // if(tempBuildBreaker != nullptr){
                        //     isBuildSideExecuted = true;
                        //     // check if columns names vectors match
                        //     const std::vector<std::string>& buildColumnNames = tempBuildBreaker->getBuildColumnsNames();
                        //     if(_isLeftSideBuildSide){
                        //         if(_joinAttributes.size() == buildColumnNames.size()){
                        //             for(uint32_t i=0; i<_joinAttributes.size(); i++){
                        //                 if(buildColumnNames[i] != (_joinAttributes[i]._leftTableAliasName + "." + _joinAttributes[i]._leftColumnPermanentName)){
                        //                    isBuildSideExecuted = false;
                        //                 }
                        //             }
                        //         }
                        //         else{
                        //             isBuildSideExecuted = false;
                        //         }
                        //     }
                        //     else{
                        //         if(_joinAttributes.size() == buildColumnNames.size()){
                        //             for(uint32_t i=0; i<_joinAttributes.size(); i++){
                        //                 if(buildColumnNames[i] != (_joinAttributes[i]._rightTableAliasName + "." + _joinAttributes[i]._rightColumnPermanentName)){
                        //                    isBuildSideExecuted = false;
                        //                 }
                        //             }
                        //         }
                        //         else{
                        //             isBuildSideExecuted = false;
                        //         }
                        //     }
                        // }
                        // if the breaker in the intermediate result node is a default breaker, contains case it is a hash join build breaker (on the right column)
                        DefaultBreaker<TABLE_PARTITION_SIZE>* tempDefaultBreaker = dynamic_cast<DefaultBreaker<TABLE_PARTITION_SIZE>*>(buildSideIntermediateResultNode->getBreaker().get());
                        if(tempDefaultBreaker != nullptr){
                            isBuildSideExecuted = true;
                        }
                    }
                }

                // in adaptive execution the probe side can be also executed i.e. the probe side is an intermediate result node
                bool isProbeSideExecuted = false;
                IntermediateResultNode<TABLE_PARTITION_SIZE>* probeSideIntermediateResultNode = dynamic_cast<IntermediateResultNode<TABLE_PARTITION_SIZE>*>(probeSidePlan);
                if(probeSideIntermediateResultNode != nullptr){
                    DefaultBreaker<TABLE_PARTITION_SIZE>* tempDefaultBreaker = dynamic_cast<DefaultBreaker<TABLE_PARTITION_SIZE>*>(probeSideIntermediateResultNode->getBreaker().get());
                    if(tempDefaultBreaker != nullptr){
                        isProbeSideExecuted = true;
                    }
                }

                // if build side executed
                if(isBuildSideExecuted){
                    // set a flag in the build side that it was already executed
                    buildSideExecution->setExecuted();
                }
                else{
                    // set this node as breaker of the build side
                    buildSideExecution->setBreakerNode(this->shared_from_this());
                }

                // in case we have an intermediate result node on the probe side, which finally causes a breaker stater pipeline
                if(isProbeSideExecuted){
                    // set a flag in the probe side that it was already executed
                    probeSideExecution->setExecuted();

                    // create a new execution graph node
                    std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> newNode(std::make_shared<ExecutionGraphNode<TABLE_PARTITION_SIZE>>());
                    newNode->addChild(buildSideExecution);
                    newNode->addUpDegree(buildSideExecution.get());
                    newNode->addChild(probeSideExecution);
                    newNode->addUpDegree(probeSideExecution.get());

                    // return the new node
                    return newNode;
                }
                // standard case
                else {
                    // set build side as child/dependency of probe side
                    probeSideExecution->addChild(buildSideExecution);

                    // maintain degree of the probe side, here we need the complete build side (also when it was already executed)
                    // because the degree is calculated bottom up
                    probeSideExecution->addUpDegree(buildSideExecution.get());

                    // return the probe side node
                    return probeSideExecution;
                }
            }



            // searches on the build side (recursive call), then on the probe side (recursive call) for an
            // unexecuted breaker, if it does find a breaker it considers itself (the build) as breaker, if it was already executed then this
            // function acts as recursion breaker and just returns a null pointer
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findDeepestUnexecutedPipeline(){
                // in case there is already a build breaker then this node and the nodes below cannot be the breaker of an unexecuted pipeline
                if(_buildBreaker != nullptr){
                    return nullptr;
                }

                // determine pointer to build and probe side
                PlanNodeBase<TABLE_PARTITION_SIZE>* buildSidePlan;
                PlanNodeBase<TABLE_PARTITION_SIZE>* probeSidePlan;
                if(_isLeftSideBuildSide){
                    buildSidePlan = _leftOptimalPlan.get();
                    probeSidePlan = _rightOptimalPlan.get();
                }
                else{
                    buildSidePlan = _rightOptimalPlan.get();
                    probeSidePlan = _leftOptimalPlan.get();
                }

                // check if there is an intermediate result node on the build side
                IntermediateResultNode<TABLE_PARTITION_SIZE>* intermediateResultNode = dynamic_cast<IntermediateResultNode<TABLE_PARTITION_SIZE>*>(buildSidePlan);
                std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> ret;
                if(intermediateResultNode != nullptr){
                    // in this case we just have to check if there is something on the probe side and we immediately return it
                    return probeSidePlan->findDeepestUnexecutedPipeline();
                }
                // if there is no intermediate result node on the build side
                else{
                    // search the build side if there is something
                    ret = buildSidePlan->findDeepestUnexecutedPipeline();
                    if(ret != nullptr){
                        return ret;
                    }
                    // if there is nothing on the build side, we check the probe side if there is something
                    ret = probeSidePlan->findDeepestUnexecutedPipeline();
                    if(ret != nullptr){
                        return ret;
                    }
                    // in case there are no unexecuted pipes/breakers on the build and probe side, then this node's breaker/pipeline is unexecuted and we return it
                    return this->shared_from_this();
                }
            }


            // implementation of interface function, adds either a hash join build breaker or hash join probe operator to the given pipeline
            // first recursively traverses down and then adds the operator (post order)
            void addOperatorToPipeline(std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>>& pipeline, uint32_t& executedOperatorsCount){
                // this is an alternative point to decide on build and probe side
                PlanNodeBase<TABLE_PARTITION_SIZE>* buildSide;
                PlanNodeBase<TABLE_PARTITION_SIZE>* probeSide;
                if(_isLeftSideBuildSide){
                    buildSide = _leftOptimalPlan.get();
                    probeSide = _rightOptimalPlan.get();
                }
                else{
                    buildSide = _rightOptimalPlan.get();
                    probeSide = _leftOptimalPlan.get();
                }

                // see if there is already an appropriate build breaker on the build side
                // TODO in case there is a build breaker on the probe side, we ignore it, maybe we should define a heuristic until when we would consider it
                if(_buildBreaker == nullptr){
                    // we check if one of the sides is already an appropriate hash join build breaker,
                    // i.e. an intermediate result node with a build breaker on the correct column
                    IntermediateResultNode<TABLE_PARTITION_SIZE>* intermediateResultNode = dynamic_cast<IntermediateResultNode<TABLE_PARTITION_SIZE>*>(buildSide);
                    if(intermediateResultNode != nullptr){
                        // if the breaker in the intermediate result node is a hash join build breaker and it was built on the right columns
                        _buildBreaker = std::dynamic_pointer_cast<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(intermediateResultNode->getBreaker());
                        if(_buildBreaker != nullptr){
                            // check if columns names vectors match
                            const std::vector<std::string>& buildColumnNames = _buildBreaker->getBuildColumnsNames();
                            if(_isLeftSideBuildSide){
                                if(_joinAttributes.size() == buildColumnNames.size()){
                                    for(uint32_t i=0; i<_joinAttributes.size(); i++){
                                        if(buildColumnNames[i] != (_joinAttributes[i]._leftTableAliasName + "." + _joinAttributes[i]._leftColumnPermanentName)){
                                           _buildBreaker = nullptr;
                                        }
                                    }
                                }
                                else{
                                    _buildBreaker = nullptr;
                                }
                            }
                            else{
                                if(_joinAttributes.size() == buildColumnNames.size()){
                                    for(uint32_t i=0; i<_joinAttributes.size(); i++){
                                        if(buildColumnNames[i] != (_joinAttributes[i]._rightTableAliasName + "." + _joinAttributes[i]._rightColumnPermanentName)){
                                           _buildBreaker = nullptr;
                                        }
                                    }
                                }
                                else{
                                    _buildBreaker = nullptr;
                                }
                            }
                            // basically removes ownership of intermediate result from intermediate result node, since we cannot use unique pointers (they cannot be casted)
                            if(_buildBreaker != nullptr){
                                intermediateResultNode->removeIntermediateResultOwnership();
                            }
                        }
                        // if we have an intermediate result node on the build side but it was no fitting hash table
                        if(_buildBreaker == nullptr){
                            // if the breaker in the intermediate result node is a default breaker
                            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> tempDefaultBreaker = std::dynamic_pointer_cast<DefaultBreaker<TABLE_PARTITION_SIZE>>(intermediateResultNode->getBreaker());
                            if(tempDefaultBreaker != nullptr){
                                // we found a breaker so add a hash join late operator/ probe operator, but first deep dive in the probe side
                                probeSide->addOperatorToPipeline(pipeline, executedOperatorsCount);
                                // we create a hash join late breaker using the default breaker
                                // TODO distinct or not distinct flag -> severe performance difference
                                std::vector<std::string> leftColumnsNames;
                                std::vector<std::string> rightColumnsNames;
                                for(const auto& joinAttribute : _joinAttributes){
                                    leftColumnsNames.push_back(joinAttribute._leftTableAliasName + "." + joinAttribute._leftColumnPermanentName);
                                    rightColumnsNames.push_back(joinAttribute._rightTableAliasName + "." + joinAttribute._rightColumnPermanentName);
                                }
                                _runtimeStatistics = std::make_unique<RuntimeStatistics>();
                                if(_isLeftSideBuildSide){
                                    pipeline->addHashJoinLateOperator(tempDefaultBreaker, leftColumnsNames, rightColumnsNames, _runtimeStatistics.get());
                                }
                                else{
                                    pipeline->addHashJoinLateOperator(tempDefaultBreaker, rightColumnsNames, leftColumnsNames, _runtimeStatistics.get());
                                }
                                // basically removes ownership of intermediate result from intermediate result node, since we cannot use unique pointers (they cannot be casted)
                                intermediateResultNode->removeIntermediateResultOwnership();
                                // increase executed operators count
                                executedOperatorsCount++;
                                #ifdef PRINT_PLAN_CREATION_INFO
                                    std::cout << "created new late probe operator 1" << std::endl;
                                #endif
                                // add projection operator
                                pipeline->addProjectionOperator();
                                return;
                            }
                        }
                    }
                }

                // in case a build breaker was set (e.g. we visited this node before or just found one)
                if(_buildBreaker != nullptr){
                    _runtimeStatistics = std::make_unique<RuntimeStatistics>();
                    // deep dive on probe side
                    probeSide->addOperatorToPipeline(pipeline, executedOperatorsCount);
                    if(_isLeftSideBuildSide){
                        // create the early hash join operator and add it to the pipeline
                        std::vector<std::string> leftColumnsNames;
                        std::vector<std::string> rightColumnsNames;
                        for(const auto& joinAttribute : _joinAttributes){
                            rightColumnsNames.push_back(joinAttribute._rightTableAliasName + "." + joinAttribute._rightColumnPermanentName);
                            leftColumnsNames.push_back(joinAttribute._leftTableAliasName + "." + joinAttribute._leftColumnPermanentName);
                        }
                        pipeline->addHashJoinEarlyOperator(_buildBreaker, leftColumnsNames, rightColumnsNames, _runtimeStatistics.get());
                        executedOperatorsCount++;
                        #ifdef PRINT_PLAN_CREATION_INFO
                            std::cout << "created new early probe operator 1" << std::endl;
                        #endif
                        // add projection operator
                        pipeline->addProjectionOperator();
                    }
                    else{
                        // create the early hash join operator and add it to the pipeline
                        std::vector<std::string> leftColumnsNames;
                        std::vector<std::string> rightColumnsNames;
                        for(const auto& joinAttribute : _joinAttributes){
                            leftColumnsNames.push_back(joinAttribute._leftTableAliasName + "." + joinAttribute._leftColumnPermanentName);
                            rightColumnsNames.push_back(joinAttribute._rightTableAliasName + "." + joinAttribute._rightColumnPermanentName);
                        }
                        pipeline->addHashJoinEarlyOperator(_buildBreaker, rightColumnsNames, leftColumnsNames, _runtimeStatistics.get());
                        executedOperatorsCount++;
                        #ifdef PRINT_PLAN_CREATION_INFO
                            std::cout << "created new early probe operator 2" << std::endl;
                        #endif
                        // add projection operator
                        pipeline->addProjectionOperator();
                    }
                    return;
                }
                // default case, create a hash join build breaker, deep dive in build side
                buildSide->addOperatorToPipeline(pipeline, executedOperatorsCount);
                // and add build breaker
                if(_isLeftSideBuildSide){
                    std::vector<std::string> leftColumnsNames;
                    for(const auto& joinAttribute : _joinAttributes){
                        leftColumnsNames.push_back(joinAttribute._leftTableAliasName + "." + joinAttribute._leftColumnPermanentName);
                    }
                    // TODO distinct or not distinct flag -> severe performance difference
                    _buildBreaker = pipeline->addHashJoinEarlyBreaker(leftColumnsNames, _leftOptimalPlan->getOutputCardinality()); // we do not give a runtime statistics object to the breaker
                    #ifdef PRINT_PLAN_CREATION_INFO
                        std::cout << "created new early breaker 1" << std::endl;
                    #endif
                }
                else {
                    std::vector<std::string> rightColumnsNames;
                    for(const auto& joinAttribute : _joinAttributes){
                        rightColumnsNames.push_back(joinAttribute._rightTableAliasName + "." + joinAttribute._rightColumnPermanentName);
                    }
                    // TODO distinct or not distinct flag -> severe performance difference
                    _buildBreaker = pipeline->addHashJoinEarlyBreaker(rightColumnsNames, _rightOptimalPlan->getOutputCardinality()); // we do not give a runtime statistics object to the breaker
                    #ifdef PRINT_PLAN_CREATION_INFO
                        std::cout << "created new early breaker 2" << std::endl;
                    #endif
                }
            }


            // implementation of 'BreakerNodeBase' interface function, return the runtime feedback that is in the build/ breaker side,
            // the 'executedPlanClassId' will also be the one of the build/ breaker side and not the one of this node
            void getRuntimeFeedback(std::bitset<64>& executedPlanClassId, uint64_t& estimatedCardinality) const {
               // determine the plan class id and its estimated cardinality from the build/breaker side
                if(_isLeftSideBuildSide){
                    executedPlanClassId = _leftOptimalPlan->getPlanClassId();
                    estimatedCardinality = _leftOptimalPlan->getOutputCardinality();
                }
                else{
                    executedPlanClassId = _rightOptimalPlan->getPlanClassId();
                    estimatedCardinality = _rightOptimalPlan->getOutputCardinality();
                }
            }

            // implementation of interface function to get the intermediate result from a breaker to link it in the plan table
            void getIntermediateResult(std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>>& intermediateResultBreaker){
                // ensure that the build breaker is set, so that we can get it
                if(_buildBreaker==nullptr){
                    std::runtime_error("Error in hash join node while getting intermediate result");
                }
                intermediateResultBreaker = std::static_pointer_cast<DefaultBreaker<TABLE_PARTITION_SIZE>>(_buildBreaker);
                // now that the intermediate result is linked into the plan table and all following plans
                // are re-enumerated, we set the pointer in this plan node dot nullptr
                // this emulate the behavior of a unique pointer, but since unique pointer cannot be casted, we have to use shared pointers
                // basically remove ownership of intermediate result from this result node
                _buildBreaker = nullptr;
            }

            // returns the pointer to the optimal sub plan on the left hand side of the join
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> getLeftOptimalPlan() {
                return _leftOptimalPlan;
            }

            // returns the pointer to the optimal sub plan on the left hand side of the join
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> getRightOptimalPlan() {
                return _rightOptimalPlan;
            }

            // returns a const reference to the join attributes of this node, used to determine all possible plans of for a query
            const std::vector<JoinAttributeMetaSimplified>& getJoinAttributes() {
                return _joinAttributes;
            }

            // returns the estimated selectivity of this join i.e. the product the join selectivities of each single join attribute
            double getEstimatedJoinSelectivity() const {
                return _estimatedJoinSelectivity;
            }

            // implementation of interface function, first dives deep into sub plans and then adds this plan
            void findAllSubPlans(std::vector<std::pair<std::bitset<64>,std::string>>& allSubPlans) const {
                // start with deep dive in left plan
                _leftOptimalPlan->findAllSubPlans(allSubPlans);
                // then dive into the right plan
                _rightOptimalPlan->findAllSubPlans(allSubPlans);
                // finally add this node
                allSubPlans.emplace_back(PlanNodeBase<TABLE_PARTITION_SIZE>::_planClassId, toString());
            }

            // implementation of interface function to recursively calculate the max depth of a query execution plan
            void getMaxDepth(uint32_t& depth) const {
                // determine max depth of left hand side plan
                uint32_t depthLeft=0;
                _leftOptimalPlan->getMaxDepth(depthLeft);
                //
                uint32_t depthRight=0;
                _rightOptimalPlan->getMaxDepth(depthRight);
                // determine max depth of right hand side plan
                if(depthLeft>depthRight){
                    depth = (depth + depthLeft + 1);
                }
                else{
                    depth = (depth + depthRight + 1);
                }
            }

            // implementation of interface function to save memory by deallocating executed operators
            void deallocateOperators(bool first){
                // in case this node breaker was just executed
                if(first){
                    // recursively invoke this function on the build side
                    if(_isLeftSideBuildSide){
                        _leftOptimalPlan->deallocateOperators(false);
                    }
                    else{
                        _rightOptimalPlan->deallocateOperators(false);
                    }
                }
                // in case this is not the first invocation i.e. we are below the breaker that was just executed
                else{
                    // we deallocate the build breaker, since it is not needed anymore
                    _buildBreaker = nullptr;
                    // we recursivly invoke this function on build and probe side, it would be sufficient to only do that on the probe side but the build
                    // side could be an intermediate result node that has a pointer that also points to '*_buildBreaker' so invoke it in both sides
                    // TODO it would be sufficient to invoke it on the build side if it is an intermediate result node
                    _leftOptimalPlan->deallocateOperators(false);
                    _rightOptimalPlan->deallocateOperators(false);
                }
            }

            // implementation of interface function to get this node's true output cardinality after execution
            uint64_t getTrueCardinality() const {
                if(_runtimeStatistics == nullptr){
                    throw std::runtime_error("Tried to access hash join node's runtime statistics although node was not executed 1");
                }
                return _runtimeStatistics->_trueCardinality;
            }

            // implementation of interface function recursively calculate this node's true cost after execution
            // we have to consider the different cost functions here
            uint64_t getTrueCost() const {
                if(_wasPhysicalEnumeration){
                    uint64_t buildSideTrueCardinality;
                    if(_isLeftSideBuildSide){
                        buildSideTrueCardinality = _leftOptimalPlan->getTrueCardinality();
                    }
                    else{
                        buildSideTrueCardinality = _rightOptimalPlan->getTrueCardinality();
                    }
                    // cost function used for physical enumeration, takes build side cardinality into account i.e. c_mm
                    return getTrueCardinality() + buildSideTrueCardinality + _leftOptimalPlan->getTrueCost() + _rightOptimalPlan->getTrueCost();
                }
                // cost function used for logical enumeration i.e. c_out
                return getTrueCardinality() + _leftOptimalPlan->getTrueCost() + _rightOptimalPlan->getTrueCost();
            }

            // implementation of interface function to mark a plan switch that was detected by the executor, needed for latex/tikz visualization
            void setPlanSwitched(bool isFirstInvocation){
                // if this is the breaker node on which we invoked this function first, we forward this call to the next node on the build side
                if(isFirstInvocation){
                    if(_isLeftSideBuildSide){
                        _leftOptimalPlan->setPlanSwitched(false);
                    }
                    else{
                        _rightOptimalPlan->setPlanSwitched(false);
                    }
                }
                // in this case we set the flag in the runtime statistics to mark that the plan was switched on the outgoing edge of this operator
                else{
                    if(_runtimeStatistics == nullptr){
                        throw std::runtime_error("Tried to access hash join node's runtime statistics although node was not executed 2");
                    }
                    _runtimeStatistics->_causedPlanSwitch = true;
                }
            }

            // implementation of interface function to get a 'tikz' string of this tree, if executed it contains true cardinality, true selectivity and true cost
            void getTikzString(
                uint32_t levelCount,
                bool isBuildSide,
                bool isJoinStatisticPrinted,
                bool isEdgeLabelWide,
                bool isEdgeLabelSloped,
                std::string& target
            ) const {
                // to achieve a readable latex file, spaces in each line depending on the current level of the tree are needed
                std::string sep;
                for (uint32_t i = 0; i <= levelCount; ++i) {
                    sep += "    "; // 4 spaces
                }
                // determine the strings for true cardinality, absolute cardinality difference, estimation error difference,
                // for the fill color of the relative estimation error difference, for the true selectivity, for the true cost
                // and the edge shape for adaptive execution
                std::string trueCardinalityString;
                std::string trueCardinalityDifferenceString;
                std::string estimationErrorString;
                std::string estimationErrorFillColorString;
                std::string trueSelectivityString;
                std::string trueCostString;
                std::string edgeString;
                if(_runtimeStatistics == nullptr){
                    trueCardinalityString = "-";
                    trueCardinalityDifferenceString = "-";
                    estimationErrorString = "-";
                    estimationErrorFillColorString = "yellow!0";
                    trueSelectivityString = "-";
                    trueCostString = "-";
                    edgeString = "edge={line width=0.5pt}, ";
                }
                else{
                    // true cardinality
                    trueCardinalityString = basis::Utilities::formatWithCommas(_runtimeStatistics->_trueCardinality);
                    // absolute cardinality difference
                    if (_runtimeStatistics->_trueCardinality >= this->_outputCardinality) {
                        trueCardinalityDifferenceString = "\\texttt{+}" + basis::Utilities::formatWithCommas(
                            _runtimeStatistics->_trueCardinality - this->_outputCardinality);
                    } else {
                        trueCardinalityDifferenceString = "\\texttt{-}" + basis::Utilities::formatWithCommas(
                            this->_outputCardinality - _runtimeStatistics->_trueCardinality);
                    }
                    // estimation error difference and fill color of the relative estimation error difference
                    PlanNodeBase<TABLE_PARTITION_SIZE>::getEstimationErrorStringAndFillColor(this->_outputCardinality, _runtimeStatistics->_trueCardinality,
                        estimationErrorString, estimationErrorFillColorString);
                    // true selectivity
                    trueSelectivityString = basis::Utilities::printValueWithPrecision(
                        static_cast<double>(_runtimeStatistics->_trueCardinality)
                         / (static_cast<double>(_leftOptimalPlan->getTrueCardinality()) * static_cast<double>(_rightOptimalPlan->getTrueCardinality())), 8);
                    // true cost
                    trueCostString = basis::Utilities::formatWithCommas(getTrueCost());
                    // edge shape for adaptive execution
                    if (_runtimeStatistics->_causedPlanSwitch) {
                        // if the plan was switched we print a dotted line instead of a solid line
                        edgeString = "edge={dotted, line width=1pt}, ";
                    } else {
                        edgeString = "edge={line width=0.5pt}, ";
                    }
                }
                // print the hash join node and the statistics into the edge above the current node
                target += sep + "[";
                if (isJoinStatisticPrinted) {
                    // if join statistics are printed, then we print first the estimated selectivity and the true selectivity of this join
                    target += "$\\hat{s}$: " + basis::Utilities::printValueWithPrecision(_estimatedJoinSelectivity, 8)
                        + " / $s$: " + trueSelectivityString;
                    // afterwards we print the estimated selectivity for each separate join including the join columns
                    // note that for a multi column join this will print more than one line
                    for (uint32_t i = 0; i < _joinAttributes.size(); ++i) {
                        target += "\\\\";
                        // // if the join is not a primary key foreign key join we will print the join columns in red color
                        // if (!_joinAttributes[i]._isPrimaryKeyForeignKeyJoin) {
                        //     target += "\\textcolor{red}{";
                        // }
                        if(_isLeftSideBuildSide){
                            target += basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._rightTableAliasName) + "."
                                + basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._rightColumnPermanentName) + " $\\bowtie$ "
                                + basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._leftTableAliasName) + "."
                                + basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._leftColumnPermanentName);
                        }
                        else{
                            target += basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._leftTableAliasName) + "."
                                + basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._leftColumnPermanentName) + " $\\bowtie$ "
                                + basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._rightTableAliasName) + "."
                                + basis::Utilities::replaceUnderscoreLatex(_joinAttributes[i]._rightColumnPermanentName);
                        }
                        target += " ($\\hat{s}$: " + basis::Utilities::printValueWithPrecision(_joinAttributes[i]._estimatedSelectivity, 8) + ")";
                        // if (!_joinAttributes[i]._isPrimaryKeyForeignKeyJoin) {
                        //     target += "}";
                        // }
                    }
                } else {
                    // otherwise we will just print the join sign in a circle shape
                    target += "$\\bowtie$ \\\\";
                }
                // print the statistics into the edge above the node
                target += ", join-node, " + edgeString;
                // edge label shape
                if (isEdgeLabelSloped && levelCount == 0) {
                    target += "edge label={[yshift=5pt]node[stats-root-node, anchor=south, ";
                } else {
                    if (isBuildSide) {
                        target += "edge label={[xshift=10pt]node[right, stats-node, ";
                        if (isEdgeLabelSloped) {
                            target += "anchor=south, ";
                        } else {
                            target += "anchor=base west, ";
                        }
                    } else {
                        target += "edge label={[xshift=-10pt]node[left, stats-node, ";
                        if (isEdgeLabelSloped) {
                            target += "anchor=south, ";
                        } else {
                            target += "anchor=base east, ";
                        }
                    }
                }
                // fill color of the edge label depending on the estimation error
                target += "fill=" + estimationErrorFillColorString + "]{";
                if (isEdgeLabelWide) {
                    // in the landscape mode we print two statistics into one line
                    target += "$\\hat{f}$: " + basis::Utilities::formatWithCommas(this->_outputCardinality) + " / $\\accentset{\\circ}{f}$: " + trueCardinalityString + "\\\\ "
                        + "$\\Delta f$: " + trueCardinalityDifferenceString + " / $q$-error: " + estimationErrorString + "\\\\ "
                        + "$\\hat{c}$: " + basis::Utilities::formatWithCommas(this->_cost) + " / $\\accentset{\\circ}{c}$: " + trueCostString + "}}\n";
                } else {
                    // in the horizontal mode we print every statistics into one line
                    target += "$\\hat{f}$: " + basis::Utilities::formatWithCommas(this->_outputCardinality) + " \\\\ $\\accentset{\\circ}{f}$: " + trueCardinalityString + "\\\\ "
                        + "$\\Delta f$: " + trueCardinalityDifferenceString + " \\\\ $q$-error: " + estimationErrorString + "\\\\ "
                        + "$\\hat{c}$: " + basis::Utilities::formatWithCommas(this->_cost) + " \\\\ $\\accentset{\\circ}{f}$: " + trueCostString + "}}\n";
                }
                // print both children of the hash join node. note that the build side is printed as the right aka second child
                if (_isLeftSideBuildSide) {
                    _rightOptimalPlan->getTikzString(levelCount+1, false, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
                    _leftOptimalPlan->getTikzString(levelCount+1, true, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
                } else {
                    _leftOptimalPlan->getTikzString(levelCount+1, false, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
                    _rightOptimalPlan->getTikzString(levelCount+1, true, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
                }
                // close the current hash join node
                target += sep + "] \n";
            }
    };


    // query execution plan node representing a default breaker, such a node is placed on top of each query execution plan
    template<uint32_t TABLE_PARTITION_SIZE>
    class FinalNode : public BreakerNodeBase<TABLE_PARTITION_SIZE>, public std::enable_shared_from_this<FinalNode<TABLE_PARTITION_SIZE>> {
        private:
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> _nextNode;
            std::vector<ProjectionMetaData> _projections;
            std::shared_ptr<ProjectionBreaker<TABLE_PARTITION_SIZE>> _breaker = nullptr;

        public:
            // constructor just passing the cost and output cardinality of 'nextNode' to the parent class constructor
            FinalNode(std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> nextNode, const std::vector<query_processor::ProjectionMetaData>& projections)
            : BreakerNodeBase<TABLE_PARTITION_SIZE>(nextNode->getOutputCardinality(), nextNode->getCost(), nextNode->getPlanClassId()), _nextNode(nextNode), _projections(projections) {
                // TODO _planClassId=nextNode->getPlanClassId() good idea!?
            }

            // implementation of interface function, get a string representation of the plan represented by this node
            std::string toString() const {
                return std::string("FinalNode[ ") + _nextNode->toString() + " ]";
            }

            // implementation of interface function to derive a pipeline dependency graph
            std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> createExecutionGraph(){
                std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> finalExecutionNode = _nextNode->createExecutionGraph();
                finalExecutionNode->setBreakerNode(this->shared_from_this());
                if(_breaker != nullptr){
                    finalExecutionNode->setExecuted();
                }
                return finalExecutionNode;
            }

            // implementation of interface function, recursively invokes this function on the next node, in case there is no
            // such breaker in the next node then it considers itself as breaker
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findDeepestUnexecutedPipeline() {
                // if there is no breaker set for this node
                if(_breaker == nullptr){
                    // check if there is an unexecuted breaker below this node, recursively invokes this function on the next node
                    std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> ret = _nextNode->findDeepestUnexecutedPipeline();
                    // return the unexecuted breaker if we found one
                    if(ret != nullptr){
                        return ret;
                    }
                    // otherwise we return this node
                    return this->shared_from_this();
                }
                // if there is already breaker set for this node, we know that there are no more unexecuted breakers below and including this node,
                // we break the recursion and return a pointer to null
                return nullptr;
            }


            // implementation of pipeline execution strategy stage 1
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findNextPipelineSimpleOrder(){
                // just forward the call to the corrensponding pure virtual function
                return findDeepestUnexecutedPipeline();
            }

            // implementation of pipeline execution strategy stage 2
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findNextPipelineMemoryEfficientOrder(){
                std::shared_ptr<ExecutionGraphNode<TABLE_PARTITION_SIZE>> executionGraph = createExecutionGraph();
                return executionGraph->findNextPipeline();
            }

            // implementation of pipeline execution strategy stage 3
            std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> findNextPipelineAdoptionFriendlyOrder(){

                // TODO
                throw std::runtime_error("nyi");

            }

            // implementation of interface function, recursivly traverses down and afterwards adds a default breaker to the pipeline
            void addOperatorToPipeline(std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>>& pipeline,  uint32_t& executedOperatorsCount) {
                // traverse down
                _nextNode->addOperatorToPipeline(pipeline, executedOperatorsCount);
                // ensure that we have a pipeline now
                if(pipeline == nullptr){
                    throw std::runtime_error("No pipeline for adding default breaker");
                }
                // ensure that the breaker was not yet created
                if(_breaker != nullptr){
                    throw std::runtime_error("Error while adding default breaker to pipeline");
                }
                // create the breaker and set the member
                _breaker = pipeline->addProjectionBreaker(_projections);
            }

            // implementation of the interface function, actually it does not make sense to retrieve
            // runtime feedback for a final node but it is implemented to e.g. see estimation error at the end of the plan
            void getRuntimeFeedback(std::bitset<64>& executedPlanClassId, uint64_t& estimatedCardinality) const {
                // plan class id and output cardiality are the same as in '_nextNode'
                executedPlanClassId = PlanNodeBase<TABLE_PARTITION_SIZE>::_planClassId;
                estimatedCardinality = PlanNodeBase<TABLE_PARTITION_SIZE>::_outputCardinality;
            }

            // implementation of interface function to get the intermediate result from a breaker to link it in the plan table
            void getIntermediateResult(std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>>& intermediateResultBreaker){
                // TODO check for DefaultBreaker and ProjectionBreaker
                // ensure that the breaker is set, so that we can get the runtime cardinality
                if(_breaker==nullptr){
                    std::runtime_error("Error in final node while getting runtime feedback");
                }
                intermediateResultBreaker = _breaker;
            }

            // the top node of each plan is a 'FinalNode' and this function provides the breaker operator containing the final result
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> getBreaker() const {
                if(_breaker==nullptr){
                    IntermediateResultNode<TABLE_PARTITION_SIZE>* tempIrNode= dynamic_cast<IntermediateResultNode<TABLE_PARTITION_SIZE>*>(_nextNode.get());
                    if(tempIrNode == nullptr){
                        std::runtime_error("Error while getting final pipeline breaker");
                    }
                    return tempIrNode->getBreaker();
                    std::runtime_error("Error while getting final pipeline breaker");
                }
                return _breaker;
            }

            // returns the pointer to the next node of this operator
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> getNextNode() const{
                return _nextNode;
            }

            // implementation of interface function, just forwards the call to '_nextNode'
            void findAllSubPlans(std::vector<std::pair<std::bitset<64>,std::string>>& allSubPlans) const {
                // just forward to first real operator in this plan
                _nextNode->findAllSubPlans(allSubPlans);
            }


            // implementation of interface function to recursively calculate the max depth of a query execution plan
            void getMaxDepth(uint32_t& depth) const {
                // ensure depth is '0' and recursively invoke this function on the next node
                depth=0;
                _nextNode->getMaxDepth(depth);
            }

            // implementation of interface function to save memory by deallocating executed operators
            void deallocateOperators(bool){
                // we will never deallocate '_breaker' breaker since this is the result and it will get deallocated when this nore gets deallocated
                // we recursivly invoke this function on '_nextNode'
                _nextNode->deallocateOperators(false);
            }

            // implementation of interface function to get this node's true output cardinality after execution
            uint64_t getTrueCardinality() const {
                return _nextNode->getTrueCardinality();
            }

            // implementation of interface function to recursively calculate this node's true cost after execution
            uint64_t getTrueCost() const {
                // get the cost of the next plan, we define to not add up the final out cardinality again
                return _nextNode->getTrueCost();
            }

            // implementation of interface function to mark a plan switch that was detected by the executor, needed for latex/tikz visualization
            void setPlanSwitched(bool){
                throw std::runtime_error("It is impossible that the plan got switched after the final node");
            }


            // implementation of interface function to get a 'tikz' string of this tree, if executed it contains true cardinality, true selectivity and true cost
            void getTikzString(
                uint32_t levelCount,
                bool isBuildSide,
                bool isJoinStatisticPrinted,
                bool isEdgeLabelWide,
                bool isEdgeLabelSloped,
                std::string& target
            ) const {
                // just forward to the next node
                return _nextNode->getTikzString(levelCount, isBuildSide, isJoinStatisticPrinted, isEdgeLabelWide, isEdgeLabelSloped, target);
            }
    };

}

#endif
