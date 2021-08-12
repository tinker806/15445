//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "common/logger.h"

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) : rows(r), cols(c)
  {
    linear = new T[r * c];
    memset(linear, 0, r * c * sizeof (T));
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() {
    delete []linear;
  }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T* [r];
    for(int i = 0; i < r; i++){
      data_[i] = this->linear + i * c;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override {
    return data_[i][j];
  }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override {
      data_[i][j] = val;
  }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    memcpy(this->linear, arr, GetColumns() * GetRows() * sizeof(T));
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override {
      delete []data_;
  };

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    auto mat1_ptr = mat1.get();
    auto mat2_ptr = mat2.get();

    if(mat1_ptr->GetRows() != mat1_ptr->GetRows()
        || mat1_ptr->GetColumns() != mat2_ptr->GetColumns()){
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<int>> new_mat{new RowMatrix<T>(mat1_ptr->GetRows(), mat1_ptr->GetRows())};
    for(int i = 0; i < mat1_ptr->GetRows(); i++){
      for(int j = 0; j < mat2_ptr->GetColumns() ; j++){
        new_mat->SetElem(i, j, mat1_ptr->GetElem(i, j) + mat2_ptr->GetElem(i, j));
      }
    }


    return new_mat;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code

    auto mat1_ptr = mat1.get();
    auto mat2_ptr = mat2.get();

    if(mat1_ptr->GetColumns() != mat2_ptr->GetRows()){
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

     auto new_mat = new RowMatrix<T>(mat1_ptr->GetRows(), mat2_ptr->GetColumns());
    for(int i = 0 ; i < mat1_ptr->GetRows(); i++){
      for(int j = 0; j < mat2_ptr->GetColumns(); j++){
        for(int p = 0; p < mat1_ptr->GetColumns();p++) {
          new_mat->SetElem(i, j, new_mat->GetElem(i, j) + mat1_ptr->GetElem(i, p) * mat2_ptr->GetElem(p, j));
        }
      }
    }


    return std::unique_ptr<RowMatrix<T>> {new_mat};
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    return AddMatrices(MultiplyMatrices(matA, matB), matC);
  }
};
}  // namespace bustub
