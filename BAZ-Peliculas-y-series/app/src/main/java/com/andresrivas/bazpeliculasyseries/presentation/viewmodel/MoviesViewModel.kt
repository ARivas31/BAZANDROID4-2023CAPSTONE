package com.andresrivas.bazpeliculasyseries.presentation.viewmodel

import android.util.Log
import androidx.lifecycle.LiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.andresrivas.common.model.LatestMoviesModel
import com.andresrivas.bazpeliculasyseries.domain.model.MovieModel
import com.andresrivas.bazpeliculasyseries.domain.model.MoviesPagesModel
import com.andresrivas.bazpeliculasyseries.domain.usecases.FavoritesUseCase
import com.andresrivas.bazpeliculasyseries.domain.usecases.GetLatestMoviesUseCase
import com.andresrivas.bazpeliculasyseries.domain.usecases.GetMovieVideoUseCase
import com.andresrivas.bazpeliculasyseries.domain.usecases.GetNowPlayingUseCase
import com.andresrivas.bazpeliculasyseries.domain.usecases.GetTopRatedUseCase
import com.andresrivas.bazpeliculasyseries.utilities.SingleLiveEvent
import com.andresrivas.common.model.MoviesVideoModel
import com.andresrivas.common.utilities.ResultAPI
import dagger.hilt.android.lifecycle.HiltViewModel
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.schedulers.Schedulers
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import javax.inject.Inject

@HiltViewModel
class MoviesViewModel @Inject constructor(
    private val latestMoviesUseCase: GetLatestMoviesUseCase,
    private val getPlayingNowUseCase: GetNowPlayingUseCase,
    private val getMovieVideoUseCase: GetMovieVideoUseCase,
    private val getTopRatedUseCase: GetTopRatedUseCase,
    private val favoritesUseCase: FavoritesUseCase,
) : ViewModel() {

    private val disposable = CompositeDisposable()

    val latestMovies: LiveData<ResultAPI<LatestMoviesModel>>
        field = SingleLiveEvent<ResultAPI<LatestMoviesModel>>()

    val movies: LiveData<ResultAPI<MoviesPagesModel>>
        field = SingleLiveEvent<ResultAPI<MoviesPagesModel>>()

    val videoMovie: LiveData<ResultAPI<MoviesVideoModel>>
        field = SingleLiveEvent<ResultAPI<MoviesVideoModel>>()

    private val _favorites = SingleLiveEvent<ResultAPI<List<MovieModel>>>()
    val favorites: LiveData<ResultAPI<List<MovieModel>>> = _favorites

    private val _isFavorite = SingleLiveEvent<ResultAPI<Boolean>>()
    val isFavorite: LiveData<ResultAPI<Boolean>> = _isFavorite

    fun getLatestMovies() {
        disposable.add(
            latestMoviesUseCase.execute()
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .subscribe({
                    latestMovies.postValue(it)
                }, {
                    Log.e("MoviesViewModel", "Error".plus(it))
                    latestMovies.postValue(ResultAPI.OnFailure(it))
                }),
        )
    }

    fun getNowPlayingMovies() {
        viewModelScope.launch(Dispatchers.IO) {
            getPlayingNowUseCase.execute().collect { movies.postValue(it) }
        }
    }

    fun getMovieVideo(idMovie: String) {
        viewModelScope.launch(Dispatchers.IO) {
            getMovieVideoUseCase.execute(GetMovieVideoUseCase.Params(idMovie)).collect {
                videoMovie.postValue(it)
            }
        }
    }

    fun getTrendingMovies() {
        viewModelScope.launch(Dispatchers.IO) {
            getTopRatedUseCase.execute().collect { movies.postValue(it) }
        }
    }

    fun getFavorites() {
        viewModelScope.launch(Dispatchers.IO) {
            favoritesUseCase.getFavoriteMovies().collect {
                _favorites.postValue(it)
            }
        }
    }

    fun isFavorite(movieModel: MovieModel) {
        viewModelScope.launch(Dispatchers.IO) {
            favoritesUseCase.isFavorite(movieModel).collect {
                _isFavorite.postValue(it)
            }
        }
    }
}
